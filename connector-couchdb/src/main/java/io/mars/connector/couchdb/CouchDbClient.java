package io.mars.connector.couchdb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Minimal CouchDB HTTP client covering exactly what the sink needs.
 *
 * <p>Uses the JDK HTTP client so the connector adds no transport dependency beyond
 * Jackson. Not thread-safe; one instance per sink writer.
 */
final class CouchDbClient implements AutoCloseable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String baseUrl;
    private final String db;
    private final String authHeader;
    private final Duration timeout;
    private final HttpClient http;

    CouchDbClient(String baseUrl, String db, String username, String password, Duration timeout) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.db = db;
        this.timeout = timeout;
        this.authHeader = (username == null || username.isEmpty())
                ? null
                : "Basic " + Base64.getEncoder().encodeToString(
                        (username + ":" + password).getBytes(StandardCharsets.UTF_8));
        this.http = HttpClient.newBuilder()
                .connectTimeout(timeout)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    /** Verifies the server is reachable and the target database exists. */
    void checkDatabaseExists() throws IOException {
        HttpResponse<String> r = send(request(dbUrl()).GET().build());
        if (r.statusCode() == 404) {
            throw new IOException("CouchDB database '" + db + "' does not exist at " + baseUrl);
        }
        if (r.statusCode() == 401 || r.statusCode() == 403) {
            throw new IOException("CouchDB rejected credentials for database '" + db
                    + "' (HTTP " + r.statusCode() + ")");
        }
        if (r.statusCode() >= 300) {
            throw new IOException("CouchDB returned HTTP " + r.statusCode()
                    + " for database '" + db + "': " + truncate(r.body()));
        }
    }

    /**
     * Writes a batch through {@code _bulk_docs}.
     *
     * @return the per-document outcomes, positionally aligned with {@code docs}
     */
    List<BulkResult> bulkDocs(List<ObjectNode> docs) throws IOException {
        ObjectNode payload = MAPPER.createObjectNode();
        ArrayNode arr = payload.putArray("docs");
        docs.forEach(arr::add);

        HttpResponse<String> r = send(request(dbUrl() + "/_bulk_docs")
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload.toString(), StandardCharsets.UTF_8))
                .build());

        if (r.statusCode() >= 300) {
            throw new IOException("_bulk_docs failed with HTTP " + r.statusCode()
                    + ": " + truncate(r.body()));
        }

        JsonNode body = MAPPER.readTree(r.body());
        List<BulkResult> out = new ArrayList<>(docs.size());
        if (!body.isArray()) {
            throw new IOException("_bulk_docs returned unexpected body: " + truncate(r.body()));
        }
        for (JsonNode n : body) {
            out.add(new BulkResult(
                    n.path("id").asText(null),
                    n.path("rev").asText(null),
                    n.path("ok").asBoolean(false),
                    n.path("error").asText(null),
                    n.path("reason").asText(null)));
        }
        if (out.size() != docs.size()) {
            throw new IOException("_bulk_docs returned " + out.size()
                    + " results for " + docs.size() + " documents");
        }
        return out;
    }

    /** Current {@code _rev} of a document, or null if it does not exist. */
    String currentRev(String docId) throws IOException {
        HttpResponse<String> r = send(request(docUrl(docId)).GET().build());
        if (r.statusCode() == 404) {
            return null;
        }
        if (r.statusCode() >= 300) {
            throw new IOException("GET " + docId + " failed with HTTP " + r.statusCode()
                    + ": " + truncate(r.body()));
        }
        return MAPPER.readTree(r.body()).path("_rev").asText(null);
    }

    private HttpRequest.Builder request(String url) {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url))
                .timeout(timeout)
                .header("Accept", "application/json");
        if (authHeader != null) {
            b.header("Authorization", authHeader);
        }
        return b;
    }

    private HttpResponse<String> send(HttpRequest req) throws IOException {
        try {
            return http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted calling CouchDB", e);
        }
    }

    private String dbUrl() {
        return baseUrl + "/" + URLEncoder.encode(db, StandardCharsets.UTF_8);
    }

    private String docUrl(String docId) {
        return dbUrl() + "/" + URLEncoder.encode(docId, StandardCharsets.UTF_8);
    }

    private static String truncate(String s) {
        if (s == null) {
            return "";
        }
        return s.length() <= 500 ? s : s.substring(0, 500) + "...";
    }

    @Override
    public void close() {
        // JDK HttpClient holds no resources needing explicit release on Java 17.
    }

    /** One entry from a {@code _bulk_docs} response. */
    static final class BulkResult implements Serializable {
        private static final long serialVersionUID = 1L;
        final String id;
        final String rev;
        final boolean ok;
        final String error;
        final String reason;

        BulkResult(String id, String rev, boolean ok, String error, String reason) {
            this.id = id;
            this.rev = rev;
            this.ok = ok;
            this.error = error;
            this.reason = reason;
        }

        boolean isConflict() {
            return "conflict".equals(error);
        }
    }
}
