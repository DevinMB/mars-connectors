package io.mars.connector.couchdb;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Computes the CouchDB {@code _id} for a document.
 *
 * <p>This is a deliberate, behaviour-for-behaviour port of {@code compute_doc_id()} in
 * {@code DevinMB/data-sinks} v1.3.4. Document ids are the sole idempotency mechanism for
 * these sinks -- the writer is at-least-once, and replays are absorbed because a replayed
 * record computes the same {@code _id} and upserts over itself. An id that differs from the
 * Python implementation by even one character silently creates duplicate documents instead,
 * so the quirks below are reproduced on purpose and should not be "cleaned up".
 */
public final class DocIdStrategy implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Returned when no deterministic id could be computed; caller falls back to an auto id. */
    public static final class Result implements Serializable {
        private static final long serialVersionUID = 1L;
        public final String id;
        public final String reason;

        Result(String id, String reason) {
            this.id = id;
            this.reason = reason;
        }

        public boolean hasId() {
            return id != null;
        }
    }

    public enum Mode {
        AUTO, KAFKA_KEY, TIMESTAMP, TIMESTAMP_CHAT, FIELD, CUSTOM;

        public static Mode from(String raw) {
            if (raw == null) {
                return KAFKA_KEY;
            }
            String v = raw.trim().toLowerCase();
            switch (v) {
                case "auto": return AUTO;
                case "kafka_key": return KAFKA_KEY;
                case "timestamp": return TIMESTAMP;
                case "timestamp_chat": return TIMESTAMP_CHAT;
                case "field": return FIELD;
                case "custom": return CUSTOM;
                default: return null;
            }
        }
    }

    private final Mode mode;
    private final String idField;
    private final String idTemplate;

    public DocIdStrategy(Mode mode, String idField, String idTemplate) {
        this.mode = mode;
        this.idField = idField == null ? "" : idField.trim();
        this.idTemplate = idTemplate == null ? "" : idTemplate.trim();
    }

    public Result compute(JsonNode doc, String kafkaKey) {
        if (mode == null) {
            // Python logs a warning and falls through to auto for this record.
            return new Result(null, "unknown_mode");
        }

        Long its = intTs(doc.path("timestamp"));
        String chatId = doc.hasNonNull("chat_id") ? asText(doc.get("chat_id")) : "unknown";

        switch (mode) {
            case AUTO:
                return new Result(null, "auto");

            case KAFKA_KEY:
                if (kafkaKey != null && !kafkaKey.isEmpty()) {
                    return new Result(sanitize(kafkaKey), "kafka_key");
                }
                return new Result(null, "kafka_key:missing");

            case TIMESTAMP:
                if (its != null) {
                    return new Result(String.valueOf(its), "timestamp");
                }
                return new Result(null, "timestamp:missing");

            case TIMESTAMP_CHAT:
                if (its != null) {
                    // NOTE: the Python does not sanitise this one. Reproduced deliberately --
                    // sanitising here would change ids for any chat_id containing '/'.
                    return new Result(its + ":" + chatId, "timestamp_chat");
                }
                return new Result(null, "timestamp_chat:missing_ts");

            case FIELD: {
                if (idField.isEmpty()) {
                    return new Result(null, "field:no_field_specified");
                }
                JsonNode val = idField.contains(".") ? getNested(doc, idField) : doc.get(idField);
                if (val != null && !val.isNull() && !asText(val).trim().isEmpty()) {
                    return new Result(sanitize(asText(val)), "field:" + idField);
                }
                return new Result(null, "field:" + idField + ":missing");
            }

            case CUSTOM: {
                Map<String, String> builtins = new LinkedHashMap<>();
                builtins.put("chat_id", chatId);
                builtins.put("int_ts", its != null ? String.valueOf(its) : "");
                builtins.put("ts", doc.hasNonNull("timestamp") ? asText(doc.get("timestamp")) : "");
                builtins.put("key", kafkaKey != null ? kafkaKey : "");
                builtins.put("last_updated", doc.hasNonNull("last_updated") ? asText(doc.get("last_updated")) : "");
                builtins.put("last_changed", doc.hasNonNull("last_changed") ? asText(doc.get("last_changed")) : "");
                builtins.put("entity_id", doc.hasNonNull("entity_id") ? asText(doc.get("entity_id")) : "");

                String rendered;
                try {
                    rendered = render(idTemplate, builtins, doc);
                } catch (IllegalArgumentException e) {
                    return new Result(null, "custom:render_error");
                }
                rendered = sanitize(rendered).trim();
                if (!rendered.isEmpty()
                        && !rendered.startsWith(":")
                        && !rendered.equals(":")
                        && !rendered.equals("::")) {
                    return new Result(rendered, "custom");
                }
                return new Result(null, "custom:empty_render");
            }

            default:
                return new Result(null, "unknown_mode");
        }
    }

    /** Port of {@code int_ts()}: coerce to whole seconds, truncating toward zero; null on failure. */
    static Long intTs(JsonNode ts) {
        if (ts == null || ts.isNull() || ts.isMissingNode()) {
            return null;
        }
        try {
            return (long) Double.parseDouble(asText(ts).trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** Port of {@code get_nested()}: walk a dotted path, null if any segment is missing. */
    static JsonNode getNested(JsonNode doc, String path) {
        if (path == null || path.isEmpty() || doc == null || !doc.isObject()) {
            return null;
        }
        JsonNode cur = doc;
        for (String part : path.split("\\.", -1)) {
            if (cur == null || !cur.isObject() || !cur.has(part)) {
                return null;
            }
            cur = cur.get(part);
        }
        return cur;
    }

    /**
     * Port of {@code sanitize_id_segment()}. CouchDB tolerates most characters in an {@code _id},
     * but '/' must be percent-encoded at every reference site, so it is stripped preemptively.
     */
    static String sanitize(String value) {
        return value == null ? "" : value.replace("/", "_");
    }

    /**
     * Emulates Python's {@code str.format} for the subset the templates use: {@code {name}}
     * placeholders plus {@code {{}} } escapes.
     *
     * <p>Unlike the Python, {@code {doc.a.b}} resolves against the document. In v1.3.4 that
     * syntax was documented but non-functional -- {@code format(**builtins)} raised {@code KeyError}
     * and the record silently fell back to an auto id. No deployed config used it, so making it
     * work cannot change any existing id.
     *
     * @throws IllegalArgumentException on an unknown placeholder or malformed template,
     *         mirroring the Python's caught-exception path.
     */
    static String render(String template, Map<String, String> builtins, JsonNode doc) {
        StringBuilder out = new StringBuilder();
        int i = 0;
        while (i < template.length()) {
            char c = template.charAt(i);
            if (c == '{') {
                if (i + 1 < template.length() && template.charAt(i + 1) == '{') {
                    out.append('{');
                    i += 2;
                    continue;
                }
                int end = template.indexOf('}', i + 1);
                if (end < 0) {
                    throw new IllegalArgumentException("unterminated placeholder in template");
                }
                String name = template.substring(i + 1, end).trim();
                if (name.isEmpty()) {
                    throw new IllegalArgumentException("empty placeholder in template");
                }
                if (builtins.containsKey(name)) {
                    out.append(builtins.get(name));
                } else if (name.startsWith("doc.")) {
                    JsonNode v = getNested(doc, name.substring(4));
                    if (v == null || v.isNull()) {
                        throw new IllegalArgumentException("unresolved path: " + name);
                    }
                    out.append(asText(v));
                } else {
                    throw new IllegalArgumentException("unknown placeholder: " + name);
                }
                i = end + 1;
            } else if (c == '}') {
                if (i + 1 < template.length() && template.charAt(i + 1) == '}') {
                    out.append('}');
                    i += 2;
                    continue;
                }
                throw new IllegalArgumentException("unmatched '}' in template");
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    /** Textual form of a node without the quotes Jackson's toString() would add. */
    static String asText(JsonNode n) {
        if (n == null || n.isNull()) {
            return "";
        }
        if (n.isFloatingPointNumber()) {
            // Jackson renders 1726300000.75 as "1.72630000075E9"; Python's str() renders it
            // as "1726300000.75". Document ids must match the Python sinks character for
            // character or a replay writes a second document instead of upserting, so
            // floating-point values are formatted the way Python would.
            return pythonFloat(n.asDouble());
        }
        return n.isValueNode() ? n.asText() : n.toString();
    }

    /**
     * Formats a double the way CPython's {@code str()} does: shortest round-trip digits,
     * fixed notation while the decimal exponent is in [-4, 16), scientific outside it.
     */
    static String pythonFloat(double d) {
        if (Double.isNaN(d)) {
            return "nan";
        }
        if (Double.isInfinite(d)) {
            return d > 0 ? "inf" : "-inf";
        }
        if (d == 0.0) {
            return (1 / d < 0) ? "-0.0" : "0.0";
        }

        // Double.toString gives the shortest round-tripping digits, just in Java's notation.
        java.math.BigDecimal bd = new java.math.BigDecimal(Double.toString(d));
        int exp = bd.precision() - bd.scale() - 1;

        if (exp < -4 || exp >= 16) {
            java.math.BigDecimal mantissa = bd.movePointLeft(exp).stripTrailingZeros();
            String m = mantissa.toPlainString();
            return m + "e" + (exp < 0 ? "-" : "+")
                    + (Math.abs(exp) < 10 ? "0" : "") + Math.abs(exp);
        }

        String s = bd.stripTrailingZeros().toPlainString();
        if (s.indexOf('.') < 0) {
            // Python always shows a float as a float.
            s = s + ".0";
        }
        return s;
    }
}
