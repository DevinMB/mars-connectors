package io.mars.connector.couchdb;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/** Table options accepted by the {@code couchdb} connector. */
public final class CouchDbOptions {

    private CouchDbOptions() {}

    public static final ConfigOption<String> URL = ConfigOptions.key("url")
            .stringType().noDefaultValue()
            .withDescription("Base URL of the CouchDB server, e.g. http://localhost:5984");

    public static final ConfigOption<String> DB = ConfigOptions.key("db")
            .stringType().noDefaultValue()
            .withDescription("Target database name. It must already exist.");

    public static final ConfigOption<String> USERNAME = ConfigOptions.key("username")
            .stringType().noDefaultValue()
            .withDescription("CouchDB user with write access to the database.");

    public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password")
            .stringType().noDefaultValue()
            .withDescription("Password for the CouchDB user.");

    public static final ConfigOption<String> ID_MODE = ConfigOptions.key("id.mode")
            .stringType().defaultValue("kafka_key")
            .withDescription("How to derive the CouchDB _id: "
                    + "auto | kafka_key | timestamp | timestamp_chat | field | custom");

    public static final ConfigOption<String> ID_FIELD = ConfigOptions.key("id.field")
            .stringType().defaultValue("")
            .withDescription("For id.mode=field: document field to use, dotted paths allowed "
                    + "(e.g. context.id).");

    public static final ConfigOption<String> ID_TEMPLATE = ConfigOptions.key("id.template")
            .stringType().defaultValue("{int_ts}:{chat_id}")
            .withDescription("For id.mode=custom: template over {chat_id} {int_ts} {ts} {key} "
                    + "{last_updated} {last_changed} {entity_id}, plus {doc.a.b} for nested fields.");

    public static final ConfigOption<String> DOC_COLUMN = ConfigOptions.key("doc.column")
            .stringType().defaultValue("doc")
            .withDescription("Name of the STRING column holding the JSON document to write.");

    public static final ConfigOption<String> KEY_COLUMN = ConfigOptions.key("key.column")
            .stringType().defaultValue("kafka_key")
            .withDescription("Name of the STRING column holding the record key. Stamped onto "
                    + "every document as 'kafka-key'. Optional.");

    public static final ConfigOption<Integer> BATCH_SIZE = ConfigOptions.key("sink.batch.size")
            .intType().defaultValue(200)
            .withDescription("Maximum documents per _bulk_docs request.");

    public static final ConfigOption<Duration> FLUSH_INTERVAL = ConfigOptions.key("sink.flush.interval")
            .durationType().defaultValue(Duration.ofSeconds(2))
            .withDescription("Flush a partial batch after this long.");

    public static final ConfigOption<Integer> MAX_RETRIES = ConfigOptions.key("sink.max-retries")
            .intType().defaultValue(5)
            .withDescription("Retries for a conflicted or failed document before giving up.");

    public static final ConfigOption<Boolean> IGNORE_WRITE_ERRORS =
            ConfigOptions.key("sink.ignore-write-errors")
            .booleanType().defaultValue(false)
            .withDescription("Log and skip documents that still fail after retries instead of "
                    + "failing the job. Off by default: silent data loss is worse than a restart.");

    public static final ConfigOption<Duration> HTTP_TIMEOUT = ConfigOptions.key("http.timeout")
            .durationType().defaultValue(Duration.ofSeconds(30))
            .withDescription("Per-request HTTP timeout.");
}
