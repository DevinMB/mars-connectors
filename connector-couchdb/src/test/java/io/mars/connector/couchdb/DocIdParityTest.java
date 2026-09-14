package io.mars.connector.couchdb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Asserts this connector computes byte-identical document ids to the Python sink it replaces.
 *
 * <p>{@code python_results.json} is a golden file produced by running {@code fixtures.json}
 * through the unmodified {@code compute_doc_id()} of {@code DevinMB/data-sinks} v1.3.4
 * (see {@code parity/run_python.py}). Ids are the only thing making these sinks idempotent
 * on replay, so a divergence here means duplicate documents rather than upserts -- silent,
 * and only visible as a growing database. Regenerate the golden file only when the Python
 * side genuinely changes, never to make this test pass.
 */
class DocIdParityTest {

    private static final ObjectMapper M = new ObjectMapper();

    @Test
    @DisplayName("document ids match the Python implementation for every fixture")
    void matchesPythonGolden() throws Exception {
        JsonNode fixtures = read("/fixtures.json");
        JsonNode golden = read("/python_results.json");

        assertEquals(fixtures.size(), golden.size(),
                "fixture and golden files are out of sync");
        assertTrue(fixtures.size() >= 40, "fixture set unexpectedly small");

        List<String> failures = new ArrayList<>();

        for (int i = 0; i < fixtures.size(); i++) {
            JsonNode f = fixtures.get(i);
            JsonNode want = golden.get(i);
            String name = f.path("n").asText();

            assertEquals(name, want.path("n").asText(), "fixture order differs from golden");

            String key = f.path("key").isNull() ? null : f.path("key").asText();
            ObjectNode doc = (ObjectNode) f.path("doc").deepCopy();
            doc.put("kafka-key", key);

            DocIdStrategy s = new DocIdStrategy(
                    DocIdStrategy.Mode.from(f.path("mode").asText()),
                    f.path("field").asText(""),
                    f.path("tpl").asText(""));
            DocIdStrategy.Result got = s.compute(doc, key);

            String wantId = want.path("id").isNull() ? null : want.path("id").asText();
            String wantReason = want.path("reason").asText();

            if (!java.util.Objects.equals(wantId, got.id)) {
                failures.add(String.format("%-28s id: python=%s java=%s",
                        name, quote(wantId), quote(got.id)));
            }
            if (!wantReason.equals(got.reason)) {
                failures.add(String.format("%-28s reason: python=%s java=%s",
                        name, wantReason, got.reason));
            }
        }

        if (!failures.isEmpty()) {
            throw new AssertionError("document id parity broken in " + failures.size()
                    + " case(s):\n  " + String.join("\n  ", failures));
        }
    }

    @Test
    @DisplayName("floats render the way Python's str() renders them")
    void pythonFloatFormatting() {
        assertEquals("1726300000.75", DocIdStrategy.pythonFloat(1726300000.75));
        assertEquals("1000000000000000.0", DocIdStrategy.pythonFloat(1e15));
        assertEquals("1e+16", DocIdStrategy.pythonFloat(1e16));
        assertEquals("0.0001", DocIdStrategy.pythonFloat(0.0001));
        assertEquals("1e-05", DocIdStrategy.pythonFloat(1e-5));
        assertEquals("1.5e-07", DocIdStrategy.pythonFloat(1.5e-7));
        assertEquals("0.1", DocIdStrategy.pythonFloat(0.1));
        assertEquals("-2.5", DocIdStrategy.pythonFloat(-2.5));
        assertEquals("0.0", DocIdStrategy.pythonFloat(0.0));
    }

    @Test
    @DisplayName("slash is stripped from id segments but not from timestamp_chat")
    void sanitisationQuirkIsPreserved() {
        // The Python sanitises kafka_key and custom output, but not timestamp_chat.
        // Reproduced deliberately; see DocIdStrategy.
        ObjectNode doc = M.createObjectNode();
        doc.put("timestamp", 5);
        doc.put("chat_id", "a/b");

        DocIdStrategy chat = new DocIdStrategy(DocIdStrategy.Mode.TIMESTAMP_CHAT, "", "");
        assertEquals("5:a/b", chat.compute(doc, "k").id);

        DocIdStrategy key = new DocIdStrategy(DocIdStrategy.Mode.KAFKA_KEY, "", "");
        assertEquals("p_q", key.compute(doc, "p/q").id);
    }

    private static JsonNode read(String resource) throws Exception {
        try (InputStream in = DocIdParityTest.class.getResourceAsStream(resource)) {
            assertNotNull(in, "missing test resource " + resource);
            return M.readTree(in);
        }
    }

    private static String quote(String s) {
        return s == null ? "None" : "'" + s + "'";
    }
}
