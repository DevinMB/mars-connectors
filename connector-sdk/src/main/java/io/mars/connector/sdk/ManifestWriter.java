package io.mars.connector.sdk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Serialises a {@link ConnectorManifest} to the JSON the console consumes.
 *
 * <p>The JSON is written by hand rather than by reflection so the wire format is
 * explicit and stable: the console is a separate product that must keep reading
 * manifests built against older SDK versions.
 */
public final class ManifestWriter {

    /** Name of the manifest resource inside a connector JAR. */
    public static final String RESOURCE_NAME = "mars-manifest.json";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ManifestWriter() {}

    public static String toJson(ConnectorManifest m) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("manifestVersion", ConnectorManifest.MANIFEST_VERSION);
        root.put("connector", m.connector());
        root.put("displayName", m.displayName());

        ArrayNode kinds = root.putArray("kinds");
        m.kinds().forEach(k -> kinds.add(k.name().toLowerCase()));

        putIfPresent(root, "description", m.description());
        putIfPresent(root, "docsUrl", m.docsUrl());
        root.put("supportsHealthCheck", m.supportsHealthCheck());

        ArrayNode options = root.putArray("options");
        for (OptionSpec o : m.options()) {
            ObjectNode n = options.addObject();
            n.put("key", o.key());
            n.put("label", o.label());
            n.put("type", o.type().name().toLowerCase());
            n.put("required", o.required());
            n.put("secret", o.secret());
            n.put("group", o.group());
            putIfPresent(n, "default", o.defaultValue());
            putIfPresent(n, "placeholder", o.placeholder());
            putIfPresent(n, "help", o.help());
            if (!o.choices().isEmpty()) {
                ArrayNode c = n.putArray("choices");
                o.choices().forEach(c::add);
            }
            if (o.dependsOn() != null) {
                ObjectNode d = n.putObject("visibleWhen");
                d.put("option", o.dependsOn());
                d.put("equals", o.dependsOnValue());
            }
        }

        ArrayNode columns = root.putArray("columns");
        for (ColumnSpec c : m.columns()) {
            ObjectNode n = columns.addObject();
            n.put("name", c.name());
            n.put("sqlType", c.sqlType());
            n.put("required", c.required());
            putIfPresent(n, "help", c.help());
        }

        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(root) + "\n";
        } catch (IOException e) {
            throw new UncheckedIOException("failed to serialise manifest for " + m.connector(), e);
        }
    }

    public static void write(ConnectorManifest m, Path outputDir) throws IOException {
        Files.createDirectories(outputDir);
        Path target = outputDir.resolve(RESOURCE_NAME);
        Files.writeString(target, toJson(m), StandardCharsets.UTF_8);
    }

    private static void putIfPresent(ObjectNode node, String field, String value) {
        if (value != null && !value.isEmpty()) {
            node.put(field, value);
        }
    }
}
