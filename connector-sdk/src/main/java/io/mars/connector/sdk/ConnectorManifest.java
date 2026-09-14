package io.mars.connector.sdk;

import java.util.List;
import java.util.Objects;

/**
 * Everything the MARS console needs in order to present a connector to a user:
 * its identity, what it can be used for, the options it accepts, and the columns
 * it expects.
 *
 * <p>This is the contract that lets a new connector appear in the console as a
 * guided form with no console-side code change. Declare it in Java next to the
 * connector's {@code ConfigOption}s; {@link ManifestWriter} emits the JSON that
 * ships inside the connector JAR as {@code /mars-manifest.json}.
 */
public final class ConnectorManifest {

    /** Bumped when the manifest shape itself changes, so the console can adapt. */
    public static final int MANIFEST_VERSION = 1;

    private final String connector;
    private final String displayName;
    private final List<ConnectorKind> kinds;
    private final String description;
    private final String docsUrl;
    private final boolean supportsHealthCheck;
    private final List<OptionSpec> options;
    private final List<ColumnSpec> columns;

    private ConnectorManifest(Builder b) {
        this.connector = Objects.requireNonNull(b.connector, "connector");
        this.displayName = b.displayName != null ? b.displayName : b.connector;
        this.kinds = b.kinds == null || b.kinds.isEmpty() ? List.of(ConnectorKind.SINK) : List.copyOf(b.kinds);
        this.description = b.description;
        this.docsUrl = b.docsUrl;
        this.supportsHealthCheck = b.supportsHealthCheck;
        this.options = b.options == null ? List.of() : List.copyOf(b.options);
        this.columns = b.columns == null ? List.of() : List.copyOf(b.columns);

        // A dependsOn pointing at a key that does not exist would render an option
        // that can never appear. Fail at build time rather than puzzle a user later.
        for (OptionSpec o : this.options) {
            if (o.dependsOn() != null
                    && this.options.stream().noneMatch(x -> x.key().equals(o.dependsOn()))) {
                throw new IllegalArgumentException(
                        "option '" + o.key() + "' depends on unknown option '" + o.dependsOn() + "'");
            }
        }
    }

    public String connector() { return connector; }
    public String displayName() { return displayName; }
    public List<ConnectorKind> kinds() { return kinds; }
    public String description() { return description; }
    public String docsUrl() { return docsUrl; }
    public boolean supportsHealthCheck() { return supportsHealthCheck; }
    public List<OptionSpec> options() { return options; }
    public List<ColumnSpec> columns() { return columns; }

    public static Builder builder(String connector) {
        return new Builder(connector);
    }

    public static final class Builder {
        private final String connector;
        private String displayName;
        private List<ConnectorKind> kinds;
        private String description;
        private String docsUrl;
        private boolean supportsHealthCheck;
        private List<OptionSpec> options;
        private List<ColumnSpec> columns;

        private Builder(String connector) { this.connector = connector; }

        public Builder displayName(String v) { this.displayName = v; return this; }
        public Builder kinds(ConnectorKind... v) { this.kinds = List.of(v); return this; }
        public Builder description(String v) { this.description = v; return this; }
        public Builder docsUrl(String v) { this.docsUrl = v; return this; }
        public Builder supportsHealthCheck(boolean v) { this.supportsHealthCheck = v; return this; }
        public Builder options(List<OptionSpec> v) { this.options = v; return this; }
        public Builder columns(List<ColumnSpec> v) { this.columns = v; return this; }

        public ConnectorManifest build() { return new ConnectorManifest(this); }
    }
}
