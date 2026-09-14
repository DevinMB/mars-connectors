package io.mars.connector.ha;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Calls Home Assistant services in response to rows.
 *
 * <p>The interesting behaviour is {@code restore-previous}. Turning a light on
 * usually means losing whatever it was doing before; restoring it afterwards means
 * reading the entity's state at turn-on and replaying it at turn-off. That is a
 * read-modify-write against a live device with state carried between two separate
 * rows, which is why it lives in a connector rather than in SQL.
 *
 * <p>The remembered state is held in the writer, not in Flink state, and is
 * therefore lost if the job restarts. That is deliberate: it matches the Python
 * controller this replaces, whose snapshot lives in an instance field and does not
 * survive a restart either. Persisting it would be worse, not better -- a snapshot
 * taken hours ago describes a light that has since been changed by somebody else.
 * After a restart the first turn-off falls back to a plain switch-off.
 */
final class HaSinkWriter implements SinkWriter<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(HaSinkWriter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final HaConfig config;
    private final HttpClient http;
    /** Entity id to the attributes it had when it was last turned on. */
    private final Map<String, ObjectNode> previous = new HashMap<>();

    private final Counter calls;
    private final Counter failures;
    private final Counter restores;

    HaSinkWriter(HaConfig config, SinkWriterMetricGroup metrics) {
        this.config = config;
        this.http = HttpClient.newBuilder().connectTimeout(config.timeout).build();
        this.calls = metrics.counter("haServiceCalls");
        this.failures = metrics.counter("haServiceFailures");
        this.restores = metrics.counter("haStateRestores");
    }

    @Override
    public void write(RowData row, Context context) throws IOException {
        if (row == null || row.isNullAt(config.actionIndex) || row.isNullAt(config.entityIndex)) {
            return;
        }
        String action = row.getString(config.actionIndex).toString().trim().toLowerCase();
        String entity = row.getString(config.entityIndex).toString();
        String attributes = (config.attributesIndex >= 0 && !row.isNullAt(config.attributesIndex))
                ? row.getString(config.attributesIndex).toString() : null;

        try {
            if ("on".equals(action)) {
                turnOn(entity, attributes);
            } else if ("off".equals(action)) {
                turnOff(entity);
            } else {
                throw new IOException("Unknown action '" + action + "'; expected 'on' or 'off'");
            }
        } catch (IOException e) {
            failures.inc();
            if (!config.ignoreErrors) {
                throw e;
            }
            LOG.error("Home Assistant call failed for {}: {}", entity, e.getMessage());
        }
    }

    private void turnOn(String entity, String attributes) throws IOException {
        if (config.restorePrevious) {
            ObjectNode snapshot = readState(entity);
            if (snapshot != null) {
                previous.put(entity, snapshot);
            }
        }

        ObjectNode payload = MAPPER.createObjectNode();
        payload.put("entity_id", entity);
        if (attributes != null && !attributes.isBlank()) {
            JsonNode extra = MAPPER.readTree(attributes);
            if (extra.isObject()) {
                extra.fields().forEachRemaining(e -> payload.set(e.getKey(), e.getValue()));
            }
        }
        call("turn_on", payload);
    }

    private void turnOff(String entity) throws IOException {
        ObjectNode snapshot = previous.remove(entity);

        // Nothing remembered -- either restore-previous is off, or this is the first
        // turn-off since the job started. A plain switch-off is the right fallback,
        // and is what the controller this replaces does when its own read fails.
        if (snapshot == null || !snapshot.path("was_on").asBoolean(false)) {
            ObjectNode payload = MAPPER.createObjectNode();
            payload.put("entity_id", entity);
            call("turn_off", payload);
            return;
        }

        // It was on before, with these attributes. Put it back rather than switching
        // it off, which is the whole point of restore-previous.
        ObjectNode payload = MAPPER.createObjectNode();
        payload.put("entity_id", entity);
        snapshot.fields().forEachRemaining(e -> {
            if (!"was_on".equals(e.getKey())) {
                payload.set(e.getKey(), e.getValue());
            }
        });
        restores.inc();
        call("turn_on", payload);
    }

    /** Reads the entity's current state. Returns null when it cannot be read. */
    private ObjectNode readState(String entity) {
        String url = config.baseUrl + "/api/states/"
                + URLEncoder.encode(entity, StandardCharsets.UTF_8);
        if (config.dryRun) {
            LOG.info("[dry-run] GET {}", url);
            return null;
        }
        try {
            HttpResponse<String> r = send(HttpRequest.newBuilder(URI.create(url))
                    .timeout(config.timeout)
                    .header("Authorization", "Bearer " + config.token)
                    .GET().build());
            if (r.statusCode() >= 300) {
                LOG.warn("Could not read state of {}: HTTP {}", entity, r.statusCode());
                return null;
            }
            JsonNode state = MAPPER.readTree(r.body());
            JsonNode attrs = state.path("attributes");

            ObjectNode snapshot = MAPPER.createObjectNode();
            snapshot.put("was_on", "on".equals(state.path("state").asText()));
            if (attrs.has("brightness") && !attrs.get("brightness").isNull()) {
                snapshot.put("brightness", attrs.get("brightness").asInt());
            }
            if (attrs.has("xy_color") && attrs.get("xy_color").isArray()
                    && attrs.get("xy_color").size() == 2) {
                snapshot.set("xy_color", attrs.get("xy_color"));
            }
            if (attrs.has("color_temp") && !attrs.get("color_temp").isNull()) {
                snapshot.put("color_temp", attrs.get("color_temp").asInt());
            }
            return snapshot;
        } catch (Exception e) {
            LOG.warn("Could not read state of {}: {} -- turn-off will switch it off instead",
                    entity, e.getMessage());
            return null;
        }
    }

    private void call(String service, ObjectNode payload) throws IOException {
        String url = config.baseUrl + "/api/services/" + config.domain + "/" + service;
        if (config.dryRun) {
            LOG.info("[dry-run] POST {} body={}", url, payload);
            return;
        }
        HttpResponse<String> r = send(HttpRequest.newBuilder(URI.create(url))
                .timeout(config.timeout)
                .header("Authorization", "Bearer " + config.token)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload.toString(), StandardCharsets.UTF_8))
                .build());
        if (r.statusCode() >= 300) {
            throw new IOException(config.domain + "." + service + " returned HTTP "
                    + r.statusCode() + ": " + truncate(r.body()));
        }
        calls.inc();
    }

    private HttpResponse<String> send(HttpRequest req) throws IOException {
        try {
            return http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted calling Home Assistant", e);
        }
    }

    private static String truncate(String s) {
        if (s == null) return "";
        return s.length() <= 300 ? s : s.substring(0, 300) + "...";
    }

    @Override
    public void flush(boolean endOfInput) {
        // Calls are made as rows arrive; nothing is buffered.
    }

    @Override
    public void close() {
        previous.clear();
    }
}
