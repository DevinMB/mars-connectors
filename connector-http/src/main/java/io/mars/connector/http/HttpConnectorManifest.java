package io.mars.connector.http;

import io.mars.connector.sdk.ColumnSpec;
import io.mars.connector.sdk.ConnectorKind;
import io.mars.connector.sdk.ConnectorManifest;
import io.mars.connector.sdk.MarsConnector;
import io.mars.connector.sdk.OptionSpec;
import io.mars.connector.sdk.OptionType;

import java.util.List;

/** Describes the HTTP sink to the MARS console. */
public final class HttpConnectorManifest implements MarsConnector {

    @Override
    public ConnectorManifest manifest() {
        return ConnectorManifest.builder(HttpDynamicTableSinkFactory.IDENTIFIER)
                .displayName("HTTP")
                .kinds(ConnectorKind.SINK)
                .description("Calls an HTTP service once per row. Use this to make a stream "
                        + "act on something: a webhook, an API, a home automation system.")
                .options(List.of(
                        OptionSpec.builder("base-url", OptionType.STRING)
                                .label("Base URL").required(true).group("Connection")
                                .placeholder("http://localhost:8123")
                                .help("Every request is sent under this URL.").build(),
                        OptionSpec.builder("auth.value", OptionType.PASSWORD)
                                .label("Credential").group("Connection")
                                .placeholder("Bearer ...")
                                .help("Sent in the Authorization header. Use a stored secret.").build(),
                        OptionSpec.builder("auth.header", OptionType.STRING)
                                .label("Credential header").group("Connection")
                                .defaultValue("Authorization").build(),
                        OptionSpec.builder("content-type", OptionType.STRING)
                                .label("Content type").group("Connection")
                                .defaultValue("application/json").build(),

                        OptionSpec.builder("method", OptionType.ENUM)
                                .label("Default method").group("Request").defaultValue("POST")
                                .choices("POST", "PUT", "PATCH", "DELETE", "GET").build(),
                        OptionSpec.builder("path.column", OptionType.STRING)
                                .label("Path column").group("Schema").defaultValue("path")
                                .help("STRING column holding the path appended to the base URL.").build(),
                        OptionSpec.builder("body.column", OptionType.STRING)
                                .label("Body column").group("Schema").defaultValue("body")
                                .help("STRING column holding the request body.").build(),
                        OptionSpec.builder("method.column", OptionType.STRING)
                                .label("Method column").group("Schema").defaultValue("method")
                                .help("Optional STRING column overriding the method per row.").build(),

                        OptionSpec.builder("dry-run", OptionType.BOOLEAN)
                                .label("Dry run").group("Safety").defaultValue("false")
                                .help("Log each request instead of sending it. Turn this on first "
                                        + "whenever the target physically does something -- an HTTP "
                                        + "call cannot be undone, and a job reading a topic from the "
                                        + "beginning calls the service once per historical record.").build(),
                        OptionSpec.builder("sink.ignore-errors", OptionType.BOOLEAN)
                                .label("Ignore errors").group("Safety").defaultValue("false")
                                .help("Log and continue when a request fails.").build(),
                        OptionSpec.builder("sink.max-retries", OptionType.INTEGER)
                                .label("Max retries").group("Safety").defaultValue("3")
                                .help("Only 5xx and transport failures are retried; a 4xx will not "
                                        + "succeed on a second attempt.").build(),
                        OptionSpec.builder("http.timeout", OptionType.DURATION)
                                .label("Timeout").group("Safety").defaultValue("10s").build()))
                .columns(List.of(
                        new ColumnSpec("path", "STRING", true,
                                "Path appended to the base URL, e.g. /api/services/light/turn_on"),
                        new ColumnSpec("body", "STRING", false, "Request body."),
                        new ColumnSpec("method", "STRING", false,
                                "Overrides the default method for this row.")))
                .build();
    }
}
