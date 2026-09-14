package io.mars.connector.sdk;

/**
 * The widget the console should render for an option.
 *
 * <p>This is a presentation hint, not a validation rule -- the connector's own
 * {@code ConfigOption} types remain authoritative at job-submission time.
 */
public enum OptionType {
    /** Single-line free text. */
    STRING,
    /** Single-line text rendered masked, and stored in the secret store. */
    PASSWORD,
    /** Multi-line free text (templates, bodies, JSON fragments). */
    TEXT,
    /** Whole number. */
    INTEGER,
    /** Checkbox. */
    BOOLEAN,
    /** A Flink duration string such as {@code 2s} or {@code 500ms}. */
    DURATION,
    /** A closed set of choices; see {@link OptionSpec#choices()}. */
    ENUM
}
