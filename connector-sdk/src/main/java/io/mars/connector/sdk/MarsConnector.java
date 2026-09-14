package io.mars.connector.sdk;

/**
 * Implemented by every MARS connector so its {@link ConnectorManifest} can be
 * discovered at build time and shipped inside the connector JAR.
 *
 * <p>Register implementations through {@code ServiceLoader} by adding the class
 * name to {@code META-INF/services/io.mars.connector.sdk.MarsConnector}.
 */
public interface MarsConnector {

    /** The manifest describing this connector to the console. */
    ConnectorManifest manifest();
}
