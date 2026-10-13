package io.mars.connector.couchdb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Guards a sink that ran 8x slower than it should across a WAN.
 *
 * <p>An upsert aggregate rewrites documents that already exist, so every write used to
 * conflict, and each conflict was resolved with its own GET for the {@code _rev}. From
 * a cluster 25 ms from CouchDB that capped buoys_hourly at ~40 documents/s. Now the
 * writer remembers the revisions it writes, and re-reads the ones it lacks in a single
 * {@code _all_docs} request.
 */
class RevisionLookupTest {

    private static final ObjectMapper M = new ObjectMapper();

    private FakeCouch couch;
    private CouchDbSinkWriter writer;

    @BeforeEach
    void start() throws IOException {
        couch = new FakeCouch();
        writer = new CouchDbSinkWriter(
                new CouchDbConfig("http://127.0.0.1:" + couch.port(), "db", null, null,
                        DocIdStrategy.Mode.FIELD, "id", "", 0, -1, 10_000,
                        Duration.ofHours(1), 5, false, Duration.ofSeconds(5), true, false),
                UnregisteredMetricsGroup.createSinkWriterMetricGroup());
    }

    @AfterEach
    void stop() throws Exception {
        writer.close();
        couch.stop();
    }

    private void writeAll(int docs, int value) throws Exception {
        for (int i = 0; i < docs; i++) {
            writer.write(GenericRowData.of(StringData.fromString(
                    "{\"id\":\"hour:" + i + "\",\"value\":" + value + "}")), null);
        }
        writer.flush(false);
    }

    @Test
    @DisplayName("conflicts on existing documents are re-read in one request, not one each")
    void batchesTheRevisionReread() throws Exception {
        for (int i = 0; i < 200; i++) {
            couch.seed("hour:" + i);
        }
        couch.counts.clear();

        writeAll(200, 1);

        assertEquals(2, couch.count("_bulk_docs"), "one write, then one retry");
        assertEquals(1, couch.count("_all_docs"), "200 conflicts, one revision read");
        assertEquals(0, couch.count("GET doc"), "no per-document GET");
        assertEquals(1, couch.value("hour:199"));
    }

    @Test
    @DisplayName("documents this writer wrote are updated without a conflict")
    void remembersItsOwnRevisions() throws Exception {
        writeAll(200, 1);
        couch.counts.clear();

        writeAll(200, 2);
        writeAll(200, 3);

        assertEquals(2, couch.count("_bulk_docs"), "one request per flush, no retries");
        assertEquals(0, couch.count("_all_docs"));
        assertEquals(0, couch.conflicts.get());
        assertEquals(3, couch.value("hour:0"));
    }

    @Test
    @DisplayName("a revision changed by someone else conflicts once and is re-read")
    void staleRevisionRecovers() throws Exception {
        writeAll(3, 1);
        couch.seed("hour:1");          // another writer moves hour:1 on
        couch.counts.clear();

        writeAll(3, 2);

        assertEquals(2, couch.count("_bulk_docs"));
        assertEquals(1, couch.count("_all_docs"));
        assertEquals(2, couch.value("hour:1"), "the stale document still gets the new value");
    }

    @Test
    @DisplayName("a document deleted behind the writer's back is recreated")
    void deletedDocumentIsRecreated() throws Exception {
        writeAll(1, 1);
        couch.delete("hour:0");
        couch.counts.clear();

        writeAll(1, 2);

        assertEquals(2, couch.value("hour:0"));
        assertEquals(1, couch.count("_all_docs"));
    }

    /**
     * Just enough of CouchDB's revision rules: a write to an existing live document must
     * carry its current {@code _rev}, and a deleted one may be recreated without one.
     */
    private static final class FakeCouch {
        final HttpServer server;
        final Map<String, ObjectNode> docs = new ConcurrentHashMap<>();
        final Map<String, AtomicInteger> counts = new ConcurrentHashMap<>();
        final AtomicInteger conflicts = new AtomicInteger();
        final Map<String, Integer> generation = new HashMap<>();

        FakeCouch() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", this::handle);
            server.start();
        }

        int port() {
            return server.getAddress().getPort();
        }

        void stop() {
            server.stop(0);
        }

        int count(String what) {
            AtomicInteger n = counts.get(what);
            return n == null ? 0 : n.get();
        }

        int value(String id) {
            return docs.get(id).path("value").asInt();
        }

        synchronized String nextRev(String id) {
            int g = generation.merge(id, 1, Integer::sum);
            return g + "-" + Integer.toHexString((id + g).hashCode());
        }

        void seed(String id) {
            ObjectNode d = docs.containsKey(id) ? docs.get(id) : M.createObjectNode().put("_id", id);
            d.put("_rev", nextRev(id));
            docs.put(id, d);
        }

        void delete(String id) {
            ObjectNode d = M.createObjectNode().put("_id", id).put("_deleted", true);
            d.put("_rev", nextRev(id));
            docs.put(id, d);
        }

        private void tally(String what) {
            counts.computeIfAbsent(what, k -> new AtomicInteger()).incrementAndGet();
        }

        private void handle(HttpExchange ex) throws IOException {
            String path = ex.getRequestURI().getPath();
            String method = ex.getRequestMethod();
            JsonNode body = "POST".equals(method)
                    ? M.readTree(ex.getRequestBody().readAllBytes()) : null;
            JsonNode reply;
            if (path.endsWith("/_bulk_docs")) {
                tally("_bulk_docs");
                reply = bulkDocs(body.path("docs"));
            } else if (path.endsWith("/_all_docs")) {
                tally("_all_docs");
                reply = allDocs(body.path("keys"));
            } else if ("/db".equals(path)) {
                reply = M.createObjectNode().put("db_name", "db");
            } else {
                tally("GET doc");
                reply = M.createObjectNode().put("error", "unexpected");
            }
            byte[] out = M.writeValueAsBytes(reply);
            ex.getResponseHeaders().add("Content-Type", "application/json");
            ex.sendResponseHeaders(reply.has("error") ? 500 : 200, out.length);
            ex.getResponseBody().write(out);
            ex.close();
        }

        private synchronized ArrayNode bulkDocs(JsonNode in) {
            ArrayNode out = M.createArrayNode();
            for (JsonNode n : in) {
                ObjectNode doc = (ObjectNode) n.deepCopy();
                String id = doc.path("_id").asText();
                ObjectNode existing = docs.get(id);
                String current = existing == null ? null : existing.path("_rev").asText();
                boolean deleted = existing != null && existing.path("_deleted").asBoolean(false);
                String sent = doc.path("_rev").asText(null);
                // Live: must name the current rev. Absent or deleted: no rev, or the tombstone's.
                boolean ok = (existing == null || deleted)
                        ? sent == null || sent.equals(current)
                        : current.equals(sent);
                if (!ok) {
                    conflicts.incrementAndGet();
                    out.addObject().put("id", id).put("error", "conflict")
                            .put("reason", "Document update conflict.");
                    continue;
                }
                String rev = nextRev(id);
                doc.put("_rev", rev);
                docs.put(id, doc);
                out.addObject().put("ok", true).put("id", id).put("rev", rev);
            }
            return out;
        }

        private synchronized ObjectNode allDocs(JsonNode keys) {
            ObjectNode out = M.createObjectNode();
            ArrayNode rows = out.putArray("rows");
            for (JsonNode k : keys) {
                String id = k.asText();
                ObjectNode existing = docs.get(id);
                if (existing == null) {
                    rows.addObject().put("key", id).put("error", "not_found");
                    continue;
                }
                ObjectNode row = rows.addObject().put("id", id).put("key", id);
                ObjectNode value = row.putObject("value").put("rev", existing.path("_rev").asText());
                if (existing.path("_deleted").asBoolean(false)) {
                    value.put("deleted", true);
                }
            }
            return out;
        }
    }
}
