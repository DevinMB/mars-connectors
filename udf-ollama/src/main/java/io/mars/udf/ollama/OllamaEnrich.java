package io.mars.udf.ollama;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Classifies a chat message with a local Ollama model and returns the enriched
 * document.
 *
 * <p>It takes and returns the whole JSON document rather than just a sentiment,
 * because the document is carried through the pipeline as an opaque string and SQL
 * has no way to merge fields into an arbitrary object. This mirrors
 * {@code enrich_record} in the Python it replaces.
 *
 * <pre>
 *   CREATE TEMPORARY FUNCTION enrich_sentiment AS 'io.mars.udf.ollama.OllamaEnrich';
 *   SELECT enrich_sentiment(doc, 'http://host:11434', 'qwen3:8b') FROM whispers_raw;
 * </pre>
 *
 * <p><b>Why this is asynchronous.</b> A classification takes seconds. A synchronous
 * UDF would block its task thread for the whole call, so throughput would be one
 * message at a time per slot. {@link AsyncScalarFunction} lets Flink keep several
 * requests in flight, bounded by {@code table.exec.async-scalar.buffer-capacity}.
 *
 * <p><b>Why it fails loudly.</b> The Python preflights the model on startup and
 * exits non-zero if it is missing. That check exists because {@code phi3:mini} was
 * removed from the Ollama host in March and every message for the next four months
 * was silently written as a regex fallback with nobody noticing. The same check
 * runs here on first use, and throws -- which fails the Flink job visibly -- rather
 * than quietly degrading.
 */
public class OllamaEnrich extends AsyncScalarFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(OllamaEnrich.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Falls back to the first {...} when a model wraps its JSON in prose. */
    private static final Pattern FIRST_OBJECT = Pattern.compile("\\{.*}", Pattern.DOTALL);

    private static final int RETRIES = 2;            // plus the initial attempt
    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final String KEEP_ALIVE = "30m";

    private transient HttpClient http;
    private transient AtomicBoolean preflightDone;
    private transient AtomicLong fallbacks;

    @Override
    public void open(FunctionContext context) {
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        this.preflightDone = new AtomicBoolean(false);
        this.fallbacks = new AtomicLong();
    }

    /**
     * @param future   completed with the enriched document
     * @param doc      the original JSON document
     * @param baseUrl  Ollama base URL, e.g. {@code http://192.168.10.8:11434}
     * @param model    model name, e.g. {@code qwen3:8b}
     */
    public void eval(CompletableFuture<String> future, String doc, String baseUrl, String model) {
        if (doc == null) {
            future.complete(null);
            return;
        }

        final ObjectNode document;
        final String text;
        try {
            JsonNode parsed = MAPPER.readTree(doc);
            if (!parsed.isObject()) {
                future.completeExceptionally(
                        new IllegalArgumentException("enrich_sentiment expects a JSON object"));
                return;
            }
            document = (ObjectNode) parsed;
            text = extractText(document);
        } catch (Exception e) {
            future.completeExceptionally(
                    new IllegalArgumentException("enrich_sentiment could not parse the document", e));
            return;
        }

        ensurePreflight(baseUrl, model);

        classify(text, baseUrl, model, 1)
                .thenApply(sentiment -> merge(document, sentiment))
                .whenComplete((enriched, error) -> {
                    if (error == null) {
                        future.complete(enriched);
                        return;
                    }
                    // Matches the Python: a failed classification degrades to regex
                    // for this record rather than failing the pipeline.
                    long n = fallbacks.incrementAndGet();
                    LOG.error("Ollama classification failed ({}); falling back to regex. "
                            + "Fallbacks so far: {}", error.getMessage(), n);
                    future.complete(merge(document, SentimentRules.regexFallback(text)));
                });
    }

    /** Port of the Python's field precedence. */
    private static String extractText(ObjectNode doc) {
        for (String field : new String[] {"message", "text", "content"}) {
            JsonNode v = doc.get(field);
            if (v != null && !v.isNull() && !v.asText().isEmpty()) {
                return v.asText();
            }
        }
        return "";
    }

    private CompletableFuture<SentimentRules.Sentiment> classify(
            String text, String baseUrl, String model, int attempt) {

        HttpRequest request;
        try {
            request = HttpRequest.newBuilder(URI.create(trim(baseUrl) + "/api/generate"))
                    .timeout(TIMEOUT)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body(text, model), StandardCharsets.UTF_8))
                    .build();
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }

        return http.sendAsync(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
                .thenCompose(response -> {
                    if (response.statusCode() >= 300) {
                        return CompletableFuture.failedFuture(new RuntimeException(
                                "Ollama returned HTTP " + response.statusCode()));
                    }
                    try {
                        return CompletableFuture.completedFuture(parse(response.body(), text, model));
                    } catch (Exception e) {
                        return CompletableFuture.failedFuture(e);
                    }
                })
                .handle((ok, err) -> {
                    if (err == null) {
                        return CompletableFuture.completedFuture(ok);
                    }
                    if (attempt <= RETRIES) {
                        LOG.warn("Ollama attempt {}/{} failed ({}); retrying.",
                                attempt, RETRIES + 1, err.getMessage());
                        return classify(text, baseUrl, model, attempt + 1);
                    }
                    return CompletableFuture.<SentimentRules.Sentiment>failedFuture(err);
                })
                .thenCompose(f -> f);
    }

    private static String body(String text, String model) {
        ObjectNode body = MAPPER.createObjectNode();
        body.put("model", model);
        body.put("prompt", SentimentRules.SYSTEM_PROMPT + "\n\nMessage:\n" + text + "\nJSON:");
        body.put("stream", false);
        ObjectNode options = body.putObject("options");
        options.put("temperature", 0);
        options.put("num_predict", 128);
        body.put("format", "json");
        body.put("keep_alive", KEEP_ALIVE);
        // qwen3 emits a <think> block that counts against num_predict, which once
        // truncated the answer and produced empty responses. Turning it off is what
        // the deployed configuration does.
        body.put("think", false);
        return body.toString();
    }

    private static SentimentRules.Sentiment parse(String responseBody, String text, String model)
            throws Exception {
        String raw = MAPPER.readTree(responseBody).path("response").asText("").trim();

        JsonNode parsed;
        try {
            parsed = MAPPER.readTree(raw);
        } catch (Exception e) {
            Matcher m = FIRST_OBJECT.matcher(raw);
            if (!m.find()) {
                throw e;
            }
            parsed = MAPPER.readTree(m.group(0));
        }

        String bucket = parsed.path("bucket").asText(null);
        double score = parsed.path("score").asDouble(0.5);
        String reason = parsed.path("reason").asText("");

        if (bucket == null || !SentimentRules.BUCKETS.contains(bucket)) {
            throw new IllegalArgumentException("Invalid bucket from model: " + bucket);
        }

        // Precedence override, exactly as the Python: horny beats the model.
        if ("horny".equals(SentimentRules.regexClassifyFirst(text)) && !"horny".equals(bucket)) {
            bucket = "horny";
            score = Math.max(score, 0.9);
            reason = (reason + " (regex horny override)").trim();
        }

        return new SentimentRules.Sentiment(bucket, score, reason, model, score);
    }

    private static String merge(ObjectNode doc, SentimentRules.Sentiment s) {
        ObjectNode out = doc.deepCopy();
        out.put("sentiment_bucket", s.bucket);
        out.put("sentiment_score", s.score);
        out.put("sentiment_reason", s.reason);
        out.put("sentiment_model", s.model);
        out.put("confidence", s.confidence);
        out.put("classified_at", System.currentTimeMillis() / 1000.0);
        return out.toString();
    }

    /**
     * Verifies the model is installed, once, before any classification is trusted.
     * Throws if it is not, which fails the job rather than degrading silently.
     */
    private void ensurePreflight(String baseUrl, String model) {
        if (preflightDone.get()) {
            return;
        }
        synchronized (this) {
            if (preflightDone.get()) {
                return;
            }
            try {
                HttpRequest tags = HttpRequest.newBuilder(URI.create(trim(baseUrl) + "/api/tags"))
                        .timeout(Duration.ofSeconds(10)).GET().build();
                HttpResponse<String> r = http.send(tags, HttpResponse.BodyHandlers.ofString());
                if (r.statusCode() >= 300) {
                    throw new IllegalStateException("Ollama /api/tags returned HTTP " + r.statusCode());
                }
                boolean found = false;
                for (JsonNode m : MAPPER.readTree(r.body()).path("models")) {
                    if (model.equals(m.path("name").asText())) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new IllegalStateException(
                            "Model '" + model + "' is not installed on " + baseUrl
                            + ". Refusing to run: without this check every message would be "
                            + "written as a regex fallback and the degradation would be silent.");
                }
                LOG.info("Ollama preflight passed: model '{}' is available at {}", model, baseUrl);
                preflightDone.set(true);
            } catch (IllegalStateException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Ollama preflight failed against " + baseUrl + ": " + e.getMessage(), e);
            }
        }
    }

    private static String trim(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }
}
