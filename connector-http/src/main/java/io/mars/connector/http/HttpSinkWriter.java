package io.mars.connector.http;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Sends one HTTP request per row.
 *
 * <p>Unlike a database sink, the effects here are not idempotent and cannot be
 * deduplicated: calling a service twice does the thing twice. Flink is at-least-once
 * by default, so a restart replays whatever was in flight, and a job reading a topic
 * from the beginning will call the service once per historical record.
 *
 * <p>That is why {@code dry-run} exists and why it should be used first whenever the
 * target does something physical.
 */
final class HttpSinkWriter implements SinkWriter<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(HttpSinkWriter.class);

    private final HttpConfig config;
    private final HttpClient http;
    private final Counter sent;
    private final Counter failed;
    private final Counter skipped;

    HttpSinkWriter(HttpConfig config, SinkWriterMetricGroup metrics) {
        this.config = config;
        this.http = HttpClient.newBuilder()
                .connectTimeout(config.timeout)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
        this.sent = metrics.counter("httpRequestsSent");
        this.failed = metrics.counter("httpRequestsFailed");
        this.skipped = metrics.counter("httpRequestsSkipped");
    }

    @Override
    public void write(RowData row, Context context) throws IOException, InterruptedException {
        if (row == null || row.isNullAt(config.pathIndex)) {
            return;
        }
        String path = row.getString(config.pathIndex).toString();
        String body = (config.bodyIndex >= 0 && !row.isNullAt(config.bodyIndex))
                ? row.getString(config.bodyIndex).toString() : null;
        String method = (config.methodIndex >= 0 && !row.isNullAt(config.methodIndex))
                ? row.getString(config.methodIndex).toString().toUpperCase()
                : config.method;

        String url = config.baseUrl + (path.startsWith("/") ? path : "/" + path);

        if (config.dryRun) {
            skipped.inc();
            LOG.info("[dry-run] {} {}{}", method, url, body == null ? "" : " body=" + body);
            return;
        }

        IOException last = null;
        for (int attempt = 1; attempt <= config.maxRetries + 1; attempt++) {
            try {
                HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url))
                        .timeout(config.timeout)
                        .header("Content-Type", config.contentType);
                if (config.authValue != null && !config.authValue.isEmpty()) {
                    b.header(config.authHeader, config.authValue);
                }
                HttpRequest.BodyPublisher publisher = body == null
                        ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8);
                b.method(method, publisher);

                HttpResponse<String> r = http.send(b.build(),
                        HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

                if (r.statusCode() < 300) {
                    sent.inc();
                    return;
                }
                // 4xx will not succeed on a retry; only 5xx and transport errors are
                // worth repeating.
                if (r.statusCode() < 500) {
                    throw new IOException(method + " " + url + " returned HTTP "
                            + r.statusCode() + ": " + truncate(r.body()));
                }
                last = new IOException(method + " " + url + " returned HTTP " + r.statusCode());
            } catch (IOException e) {
                last = e;
                if (e.getMessage() != null && e.getMessage().contains("returned HTTP 4")) {
                    break;
                }
            }
            if (attempt <= config.maxRetries) {
                Thread.sleep(250L * attempt);
            }
        }

        failed.inc();
        if (config.ignoreErrors) {
            LOG.error("HTTP request failed and was skipped: {}", last == null ? "?" : last.getMessage());
            return;
        }
        throw last != null ? last : new IOException("HTTP request failed: " + url);
    }

    @Override
    public void flush(boolean endOfInput) {
        // Requests are sent synchronously as rows arrive; nothing is buffered.
    }

    @Override
    public void close() {
        // The JDK client holds no resources needing explicit release on Java 17.
    }

    private static String truncate(String s) {
        if (s == null) return "";
        return s.length() <= 300 ? s : s.substring(0, 300) + "...";
    }
}
