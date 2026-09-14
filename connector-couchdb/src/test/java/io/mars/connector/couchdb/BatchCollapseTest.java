package io.mars.connector.couchdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards a failure that took a live job down.
 *
 * <p>A current-state projection maps many records onto one document -- id.mode=field
 * on a device id, say. Several writes for the same _id then land in one _bulk_docs
 * request; CouchDB accepts the first and rejects the rest as conflicts, and retrying
 * cannot help because re-reading _rev still leaves two documents competing for one id
 * in the same request. The batch conflicts until the retry budget runs out and the
 * job dies on its next checkpoint with "Gave up resolving N CouchDB conflict(s)".
 */
class BatchCollapseTest {

    private static final ObjectMapper M = new ObjectMapper();

    private static ObjectNode doc(String id, int value) {
        ObjectNode n = M.createObjectNode();
        if (id != null) {
            n.put("_id", id);
        }
        n.put("value", value);
        return n;
    }

    @Test
    @DisplayName("several writes for one id collapse to the last")
    void collapsesToLast() {
        List<ObjectNode> out = CouchDbSinkWriter.collapseById(
                List.of(doc("chair", 1), doc("chair", 2), doc("chair", 3)));

        assertEquals(1, out.size(), "one document should survive, not three");
        assertEquals(3, out.get(0).get("value").asInt(),
                "the last record wins, matching what sequential upserts would leave");
    }

    @Test
    @DisplayName("distinct ids are all kept, in order")
    void keepsDistinctIds() {
        List<ObjectNode> out = CouchDbSinkWriter.collapseById(
                List.of(doc("a", 1), doc("b", 2), doc("c", 3)));

        assertEquals(3, out.size());
        assertEquals(List.of("a", "b", "c"),
                out.stream().map(d -> d.get("_id").asText()).toList());
    }

    @Test
    @DisplayName("a mixed batch keeps order and collapses only the repeats")
    void mixedBatch() {
        List<ObjectNode> out = CouchDbSinkWriter.collapseById(
                List.of(doc("a", 1), doc("b", 1), doc("a", 2), doc("c", 1), doc("b", 2)));

        assertEquals(3, out.size());
        // First-seen ordering is preserved even though the values are the later ones.
        assertEquals(List.of("a", "b", "c"),
                out.stream().map(d -> d.get("_id").asText()).toList());
        assertEquals(2, out.get(0).get("value").asInt());
        assertEquals(2, out.get(1).get("value").asInt());
    }

    @Test
    @DisplayName("server-assigned ids are never collapsed")
    void autoIdsSurvive() {
        // Documents with no _id each become a distinct document, so collapsing them
        // would silently drop records.
        List<ObjectNode> out = CouchDbSinkWriter.collapseById(
                List.of(doc(null, 1), doc(null, 2), doc(null, 3)));

        assertEquals(3, out.size());
        assertTrue(out.stream().noneMatch(d -> d.has("_id")));
    }

    @Test
    @DisplayName("deterministic and auto ids mix safely")
    void mixedWithAutoIds() {
        List<ObjectNode> out = CouchDbSinkWriter.collapseById(
                List.of(doc("a", 1), doc(null, 9), doc("a", 2), doc(null, 8)));

        assertEquals(3, out.size());
        assertEquals("a", out.get(0).get("_id").asText());
        assertEquals(2, out.get(0).get("value").asInt());
        assertEquals(2, out.stream().filter(d -> !d.has("_id")).count());
    }

    @Test
    void emptyBatch() {
        assertEquals(0, CouchDbSinkWriter.collapseById(List.of()).size());
    }
}
