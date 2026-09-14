package io.mars.connector.http;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/** Table options accepted by the {@code http} connector. */
public final class HttpOptions {

    private HttpOptions() {}

    public static final ConfigOption<String> BASE_URL = ConfigOptions.key("base-url")
            .stringType().noDefaultValue()
            .withDescription("Base URL every request is sent to, e.g. http://host:8123");

    public static final ConfigOption<String> METHOD = ConfigOptions.key("method")
            .stringType().defaultValue("POST")
            .withDescription("HTTP method used when the row carries no method column.");

    public static final ConfigOption<String> AUTH_HEADER = ConfigOptions.key("auth.header")
            .stringType().defaultValue("Authorization")
            .withDescription("Header carrying the credential.");

    public static final ConfigOption<String> AUTH_VALUE = ConfigOptions.key("auth.value")
            .stringType().noDefaultValue()
            .withDescription("Credential value, e.g. 'Bearer <token>'. Use a secret reference.");

    public static final ConfigOption<String> CONTENT_TYPE = ConfigOptions.key("content-type")
            .stringType().defaultValue("application/json");

    public static final ConfigOption<String> PATH_COLUMN = ConfigOptions.key("path.column")
            .stringType().defaultValue("path")
            .withDescription("STRING column holding the path appended to base-url.");

    public static final ConfigOption<String> BODY_COLUMN = ConfigOptions.key("body.column")
            .stringType().defaultValue("body")
            .withDescription("STRING column holding the request body. Optional.");

    public static final ConfigOption<String> METHOD_COLUMN = ConfigOptions.key("method.column")
            .stringType().defaultValue("method")
            .withDescription("STRING column overriding the method per row. Optional.");

    public static final ConfigOption<Integer> MAX_RETRIES = ConfigOptions.key("sink.max-retries")
            .intType().defaultValue(3)
            .withDescription("Retries for a failed request before the job fails.");

    public static final ConfigOption<Duration> TIMEOUT = ConfigOptions.key("http.timeout")
            .durationType().defaultValue(Duration.ofSeconds(10));

    public static final ConfigOption<Boolean> DRY_RUN = ConfigOptions.key("dry-run")
            .booleanType().defaultValue(false)
            .withDescription("Log each request instead of sending it. Use this before "
                    + "pointing a job at anything that physically does something.");

    public static final ConfigOption<Boolean> IGNORE_ERRORS =
            ConfigOptions.key("sink.ignore-errors")
            .booleanType().defaultValue(false)
            .withDescription("Log and continue when a request fails, instead of failing "
                    + "the job.");
}
