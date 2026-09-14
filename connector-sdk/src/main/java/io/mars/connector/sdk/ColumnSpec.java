package io.mars.connector.sdk;

import java.util.Objects;

/**
 * A table column the connector expects in {@code CREATE TABLE}. The console uses
 * these to pre-fill the schema when a user creates a source or sink from a form.
 */
public final class ColumnSpec {

    private final String name;
    private final String sqlType;
    private final boolean required;
    private final String help;

    public ColumnSpec(String name, String sqlType, boolean required, String help) {
        this.name = Objects.requireNonNull(name, "name");
        this.sqlType = Objects.requireNonNull(sqlType, "sqlType");
        this.required = required;
        this.help = help;
    }

    public String name() { return name; }
    public String sqlType() { return sqlType; }
    public boolean required() { return required; }
    public String help() { return help; }
}
