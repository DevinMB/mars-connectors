package io.mars.connector.couchdb;

import io.mars.connector.sdk.ColumnSpec;
import io.mars.connector.sdk.ConnectorKind;
import io.mars.connector.sdk.ConnectorManifest;
import io.mars.connector.sdk.MarsConnector;
import io.mars.connector.sdk.OptionSpec;
import io.mars.connector.sdk.OptionType;

import java.util.List;

/** Describes the CouchDB sink to the MARS console so it can render a guided form. */
public final class CouchDbConnectorManifest implements MarsConnector {

    @Override
    public ConnectorManifest manifest() {
        return ConnectorManifest.builder(CouchDbDynamicTableSinkFactory.IDENTIFIER)
                .displayName("CouchDB")
                .kinds(ConnectorKind.SINK)
                .description("Writes each row's JSON document to a CouchDB database, using a "
                        + "deterministic document id so replays upsert instead of duplicating.")
                .supportsHealthCheck(true)
                .options(List.of(
                        OptionSpec.builder("url", OptionType.STRING)
                                .label("Server URL").required(true).group("Connection")
                                .placeholder("http://localhost:5984")
                                .help("Base URL of the CouchDB server.").build(),
                        OptionSpec.builder("db", OptionType.STRING)
                                .label("Database").required(true).group("Connection")
                                .help("Target database. It must already exist.").build(),
                        OptionSpec.builder("username", OptionType.STRING)
                                .label("Username").group("Connection").build(),
                        OptionSpec.builder("password", OptionType.PASSWORD)
                                .label("Password").group("Connection").build(),

                        OptionSpec.builder("id.mode", OptionType.ENUM)
                                .label("Document id strategy").group("Document id")
                                .defaultValue("kafka_key")
                                .choices("auto", "kafka_key", "timestamp", "timestamp_chat", "field", "custom")
                                .help("How the CouchDB _id is derived. Anything other than 'auto' "
                                        + "makes writes idempotent on replay.").build(),
                        OptionSpec.builder("id.field", OptionType.STRING)
                                .label("Id field").group("Document id")
                                .visibleWhen("id.mode", "field")
                                .placeholder("context.id")
                                .help("Document field to use. Dotted paths are supported.").build(),
                        OptionSpec.builder("id.template", OptionType.STRING)
                                .label("Id template").group("Document id")
                                .visibleWhen("id.mode", "custom")
                                .defaultValue("{int_ts}:{chat_id}")
                                .help("Tokens: {chat_id} {int_ts} {ts} {key} {last_updated} "
                                        + "{last_changed} {entity_id}, plus {doc.a.b} for nested fields.").build(),

                        OptionSpec.builder("doc.column", OptionType.STRING)
                                .label("Document column").group("Schema").defaultValue("doc")
                                .help("STRING column holding the JSON document to write.").build(),
                        OptionSpec.builder("key.column", OptionType.STRING)
                                .label("Key column").group("Schema").defaultValue("kafka_key")
                                .help("STRING column holding the record key. Stamped onto every "
                                        + "document as 'kafka-key'.").build(),

                        OptionSpec.builder("sink.batch.size", OptionType.INTEGER)
                                .label("Batch size").group("Write behaviour").defaultValue("200")
                                .help("Maximum documents per _bulk_docs request.").build(),
                        OptionSpec.builder("sink.flush.interval", OptionType.DURATION)
                                .label("Flush interval").group("Write behaviour").defaultValue("2s")
                                .help("Flush a partial batch after this long.").build(),
                        OptionSpec.builder("sink.max-retries", OptionType.INTEGER)
                                .label("Max retries").group("Write behaviour").defaultValue("5")
                                .help("Attempts to resolve a document conflict before failing.").build(),
                        OptionSpec.builder("sink.ignore-write-errors", OptionType.BOOLEAN)
                                .label("Ignore write errors").group("Write behaviour").defaultValue("false")
                                .help("Log and skip failed documents instead of failing the job. "
                                        + "Leaving this off is strongly recommended.").build(),
                        OptionSpec.builder("http.timeout", OptionType.DURATION)
                                .label("HTTP timeout").group("Write behaviour").defaultValue("30s").build()))
                .columns(List.of(
                        new ColumnSpec("doc", "STRING", true,
                                "The complete JSON document to write, as a string."),
                        new ColumnSpec("kafka_key", "STRING", false,
                                "Record key. Required when the id strategy is 'kafka_key'.")))
                .build();
    }
}
