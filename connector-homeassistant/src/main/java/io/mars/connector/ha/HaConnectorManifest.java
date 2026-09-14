package io.mars.connector.ha;

import io.mars.connector.sdk.ColumnSpec;
import io.mars.connector.sdk.ConnectorKind;
import io.mars.connector.sdk.ConnectorManifest;
import io.mars.connector.sdk.MarsConnector;
import io.mars.connector.sdk.OptionSpec;
import io.mars.connector.sdk.OptionType;

import java.util.List;

/** Describes the Home Assistant sink to the MARS console. */
public final class HaConnectorManifest implements MarsConnector {

    @Override
    public ConnectorManifest manifest() {
        return ConnectorManifest.builder(HaDynamicTableSinkFactory.IDENTIFIER)
                .displayName("Home Assistant")
                .kinds(ConnectorKind.SINK)
                .description("Turns entities on and off in response to a stream. Optionally "
                        + "remembers what an entity was doing before it was turned on, and puts "
                        + "it back afterwards.")
                .docsUrl("https://developers.home-assistant.io/docs/api/rest/")
                .options(List.of(
                        OptionSpec.builder("base-url", OptionType.STRING)
                                .label("Home Assistant URL").required(true).group("Connection")
                                .placeholder("http://homeassistant.local:8123").build(),
                        OptionSpec.builder("token", OptionType.PASSWORD)
                                .label("Long-lived token").required(true).group("Connection")
                                .help("Create one under your Home Assistant profile. Store it as "
                                        + "a secret.").build(),
                        OptionSpec.builder("domain", OptionType.ENUM)
                                .label("Domain").group("Connection").defaultValue("light")
                                .choices("light", "switch", "fan", "media_player", "scene", "script")
                                .help("Which service domain to call.").build(),

                        OptionSpec.builder("restore-previous", OptionType.BOOLEAN)
                                .label("Restore previous state").group("Behaviour")
                                .defaultValue("false")
                                .help("Read the entity's state before turning it on and put it "
                                        + "back when turning it off, instead of just switching it "
                                        + "off. Costs one extra read per turn-on. The memory lives "
                                        + "in the running job, so the first turn-off after a "
                                        + "restart falls back to switching off.").build(),

                        OptionSpec.builder("action.column", OptionType.STRING)
                                .label("Action column").group("Schema").defaultValue("action")
                                .help("STRING column holding 'on' or 'off'.").build(),
                        OptionSpec.builder("entity.column", OptionType.STRING)
                                .label("Entity column").group("Schema").defaultValue("entity_id")
                                .help("STRING column holding the entity to act on.").build(),
                        OptionSpec.builder("attributes.column", OptionType.STRING)
                                .label("Attributes column").group("Schema").defaultValue("attributes")
                                .help("Optional JSON merged into a turn_on call, e.g. "
                                        + "{\"brightness\":254}.").build(),

                        OptionSpec.builder("dry-run", OptionType.BOOLEAN)
                                .label("Dry run").group("Safety").defaultValue("false")
                                .help("Log each intended call instead of making it. Turn this on "
                                        + "first: these calls affect physical things and cannot be "
                                        + "undone.").build(),
                        OptionSpec.builder("sink.ignore-errors", OptionType.BOOLEAN)
                                .label("Ignore errors").group("Safety").defaultValue("true")
                                .help("On by default: an unreachable bulb should not take down a "
                                        + "pipeline.").build(),
                        OptionSpec.builder("http.timeout", OptionType.DURATION)
                                .label("Timeout").group("Safety").defaultValue("10s").build()))
                .columns(List.of(
                        new ColumnSpec("action", "STRING", true, "'on' or 'off'."),
                        new ColumnSpec("entity_id", "STRING", true,
                                "The entity to act on, e.g. light.kitchen."),
                        new ColumnSpec("attributes", "STRING", false,
                                "JSON attributes for a turn_on call.")))
                .build();
    }
}
