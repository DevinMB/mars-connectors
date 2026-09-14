package io.mars.connector.sdk;

/** Whether a connector can be used as a source, a sink, or both. */
public enum ConnectorKind {
    SOURCE,
    SINK,
    LOOKUP
}
