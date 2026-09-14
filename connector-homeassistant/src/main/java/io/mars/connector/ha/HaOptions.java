package io.mars.connector.ha;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/** Table options accepted by the {@code home-assistant} connector. */
public final class HaOptions {

    private HaOptions() {}

    public static final ConfigOption<String> BASE_URL = ConfigOptions.key("base-url")
            .stringType().noDefaultValue()
            .withDescription("Home Assistant base URL, e.g. http://homeassistant.local:8123");

    public static final ConfigOption<String> TOKEN = ConfigOptions.key("token")
            .stringType().noDefaultValue()
            .withDescription("Long-lived access token. Use a stored secret.");

    public static final ConfigOption<String> DOMAIN = ConfigOptions.key("domain")
            .stringType().defaultValue("light")
            .withDescription("Service domain to call, e.g. light, switch, scene.");

    public static final ConfigOption<String> ACTION_COLUMN = ConfigOptions.key("action.column")
            .stringType().defaultValue("action")
            .withDescription("STRING column holding 'on' or 'off'.");

    public static final ConfigOption<String> ENTITY_COLUMN = ConfigOptions.key("entity.column")
            .stringType().defaultValue("entity_id")
            .withDescription("STRING column holding the entity to act on.");

    public static final ConfigOption<String> ATTRIBUTES_COLUMN =
            ConfigOptions.key("attributes.column")
            .stringType().defaultValue("attributes")
            .withDescription("Optional STRING column of JSON attributes merged into a turn_on "
                    + "call, e.g. {\"brightness\":254,\"xy_color\":[0.27,0.11]}.");

    public static final ConfigOption<Boolean> RESTORE_PREVIOUS =
            ConfigOptions.key("restore-previous")
            .booleanType().defaultValue(false)
            .withDescription("Read the entity's state before turning it on, and restore that "
                    + "state when turning it off, instead of simply switching it off. "
                    + "Costs one extra GET per turn_on.");

    public static final ConfigOption<Boolean> DRY_RUN = ConfigOptions.key("dry-run")
            .booleanType().defaultValue(false)
            .withDescription("Log each intended call instead of making it. Use this before "
                    + "pointing a job at anything physical.");

    public static final ConfigOption<Boolean> IGNORE_ERRORS =
            ConfigOptions.key("sink.ignore-errors")
            .booleanType().defaultValue(true)
            .withDescription("Log and continue when a call fails. On by default: a lighting "
                    + "job should not take down a pipeline because a bulb was unreachable.");

    public static final ConfigOption<Duration> TIMEOUT = ConfigOptions.key("http.timeout")
            .durationType().defaultValue(Duration.ofSeconds(10));
}
