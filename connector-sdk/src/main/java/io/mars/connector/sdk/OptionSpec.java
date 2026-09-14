package io.mars.connector.sdk;

import java.util.List;
import java.util.Objects;

/**
 * One configurable option on a connector, described richly enough that the MARS
 * console can render a labelled form field for it without knowing anything about
 * the connector itself.
 *
 * <p>Build with {@link #builder(String, OptionType)}.
 */
public final class OptionSpec {

    private final String key;
    private final OptionType type;
    private final String label;
    private final boolean required;
    private final boolean secret;
    private final String defaultValue;
    private final String placeholder;
    private final String help;
    private final String group;
    private final List<String> choices;
    private final String dependsOn;
    private final String dependsOnValue;

    private OptionSpec(Builder b) {
        this.key = Objects.requireNonNull(b.key, "key");
        this.type = Objects.requireNonNull(b.type, "type");
        this.label = b.label != null ? b.label : humanise(b.key);
        this.required = b.required;
        this.secret = b.secret || b.type == OptionType.PASSWORD;
        this.defaultValue = b.defaultValue;
        this.placeholder = b.placeholder;
        this.help = b.help;
        this.group = b.group != null ? b.group : "General";
        this.choices = b.choices == null ? List.of() : List.copyOf(b.choices);
        this.dependsOn = b.dependsOn;
        this.dependsOnValue = b.dependsOnValue;

        if (this.type == OptionType.ENUM && this.choices.isEmpty()) {
            throw new IllegalArgumentException("ENUM option '" + key + "' declares no choices");
        }
        if (this.dependsOn != null && this.dependsOnValue == null) {
            throw new IllegalArgumentException("option '" + key + "' has dependsOn without dependsOnValue");
        }
    }

    /** Turns {@code sink.batch.size} into {@code Sink batch size} for a default label. */
    private static String humanise(String key) {
        String flat = key.replace('.', ' ').replace('-', ' ').replace('_', ' ').trim();
        if (flat.isEmpty()) {
            return key;
        }
        return Character.toUpperCase(flat.charAt(0)) + flat.substring(1);
    }

    public String key() { return key; }
    public OptionType type() { return type; }
    public String label() { return label; }
    public boolean required() { return required; }
    public boolean secret() { return secret; }
    public String defaultValue() { return defaultValue; }
    public String placeholder() { return placeholder; }
    public String help() { return help; }
    public String group() { return group; }
    public List<String> choices() { return choices; }

    /** Key of another option this one is only relevant for; {@code null} if always shown. */
    public String dependsOn() { return dependsOn; }

    /** Value {@link #dependsOn()} must hold for this option to apply. */
    public String dependsOnValue() { return dependsOnValue; }

    public static Builder builder(String key, OptionType type) {
        return new Builder(key, type);
    }

    public static final class Builder {
        private final String key;
        private final OptionType type;
        private String label;
        private boolean required;
        private boolean secret;
        private String defaultValue;
        private String placeholder;
        private String help;
        private String group;
        private List<String> choices;
        private String dependsOn;
        private String dependsOnValue;

        private Builder(String key, OptionType type) {
            this.key = key;
            this.type = type;
        }

        public Builder label(String v) { this.label = v; return this; }
        public Builder required(boolean v) { this.required = v; return this; }
        public Builder secret(boolean v) { this.secret = v; return this; }
        public Builder defaultValue(String v) { this.defaultValue = v; return this; }
        public Builder placeholder(String v) { this.placeholder = v; return this; }
        public Builder help(String v) { this.help = v; return this; }
        public Builder group(String v) { this.group = v; return this; }
        public Builder choices(String... v) { this.choices = List.of(v); return this; }

        /** Only show this option when {@code otherKey} equals {@code value}. */
        public Builder visibleWhen(String otherKey, String value) {
            this.dependsOn = otherKey;
            this.dependsOnValue = value;
            return this;
        }

        public OptionSpec build() { return new OptionSpec(this); }
    }
}
