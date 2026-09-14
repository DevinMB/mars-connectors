package io.mars.connector.couchdb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Runs the shared fixture set through {@link DocIdStrategy} and writes the results in the
 * same shape the Python harness emits, so the two can be diffed. Not a unit test -- this is
 * the cross-language parity check invoked from the build.
 */
public final class ParityHarness {

    private static final ObjectMapper M = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        Path in = Path.of(args[0]);
        Path out = Path.of(args[1]);

        JsonNode fixtures = M.readTree(Files.readString(in));
        ArrayNode results = M.createArrayNode();

        for (JsonNode f : fixtures) {
            String mode = f.path("mode").asText();
            String field = f.path("field").asText("");
            String tpl = f.path("tpl").asText("");
            String key = f.path("key").isNull() ? null : f.path("key").asText();

            ObjectNode doc = (ObjectNode) f.path("doc").deepCopy();
            doc.put("kafka-key", key);

            DocIdStrategy s = new DocIdStrategy(DocIdStrategy.Mode.from(mode), field, tpl);
            DocIdStrategy.Result r = s.compute(doc, key);

            ObjectNode n = results.addObject();
            n.put("n", f.path("n").asText());
            if (r.id == null) {
                n.putNull("id");
            } else {
                n.put("id", r.id);
            }
            n.put("reason", r.reason);
        }

        Files.writeString(out, M.writerWithDefaultPrettyPrinter().writeValueAsString(results));
        System.out.println("[parity] wrote " + results.size() + " results to " + out);
    }
}
