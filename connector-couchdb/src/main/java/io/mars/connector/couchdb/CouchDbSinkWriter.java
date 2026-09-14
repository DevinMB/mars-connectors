package io.mars.connector.couchdb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Buffers rows and writes them to CouchDB through {@code _bulk_docs}.
 *
 * <p>Delivery is at-least-once. That is safe precisely because {@link DocIdStrategy}
 * produces a deterministic {@code _id}: a replayed record upserts over its own earlier
 * write rather than creating a second document. Records for which no deterministic id
 * can be computed are inserted with a server-generated id and are therefore <em>not</em>
 * idempotent -- the same trade-off the Python sinks have always made.
 */
final class CouchDbSinkWriter implements SinkWriter<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(CouchDbSinkWriter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final CouchDbClient client;
    private final DocIdStrategy idStrategy;
    private final int docIndex;
    private final int keyIndex;
    private final int batchSize;
    private final long flushIntervalMillis;
    private final int maxRetries;
    private final boolean ignoreWriteErrors;

    private final List<ObjectNode> buffer = new ArrayList<>();
    private long lastFlush = System.currentTimeMillis();

    private final Counter documentsWritten;
    private final Counter conflictsResolved;
    private final Counter documentsFailed;
    private final Counter autoIdFallbacks;

    CouchDbSinkWriter(CouchDbConfig config, SinkWriterMetricGroup metricGroup) throws IOException {
        this.client = new CouchDbClient(
                config.url, config.db, config.username, config.password, config.httpTimeout);
        this.idStrategy = new DocIdStrategy(config.idMode, config.idField, config.idTemplate);
        this.docIndex = config.docIndex;
        this.keyIndex = config.keyIndex;
        this.batchSize = config.batchSize;
        this.flushIntervalMillis = config.flushInterval.toMillis();
        this.maxRetries = config.maxRetries;
        this.ignoreWriteErrors = config.ignoreWriteErrors;

        // Fail fast on a missing database or bad credentials rather than at the first flush,
        // so a misconfigured job dies at submission instead of minutes later.
        this.client.checkDatabaseExists();

        this.documentsWritten = metricGroup.counter("couchdbDocumentsWritten");
        this.conflictsResolved = metricGroup.counter("couchdbConflictsResolved");
        this.documentsFailed = metricGroup.counter("couchdbDocumentsFailed");
        this.autoIdFallbacks = metricGroup.counter("couchdbAutoIdFallbacks");
    }

    @Override
    public void write(RowData element, Context context) throws IOException, InterruptedException {
        if (element == null || element.isNullAt(docIndex)) {
            return;
        }
        String rawDoc = element.getString(docIndex).toString();
        String kafkaKey = (keyIndex >= 0 && !element.isNullAt(keyIndex))
                ? element.getString(keyIndex).toString()
                : null;

        JsonNode parsed;
        try {
            parsed = MAPPER.readTree(rawDoc);
        } catch (IOException e) {
            documentsFailed.inc();
            if (ignoreWriteErrors) {
                LOG.error("Skipping unparseable document (key={}): {}", kafkaKey, e.getMessage());
                return;
            }
            throw new IOException("Document column did not contain valid JSON (key=" + kafkaKey + ")", e);
        }
        if (!parsed.isObject()) {
            documentsFailed.inc();
            if (ignoreWriteErrors) {
                LOG.error("Skipping non-object document (key={})", kafkaKey);
                return;
            }
            throw new IOException("Document column must contain a JSON object (key=" + kafkaKey + ")");
        }

        ObjectNode doc = (ObjectNode) parsed;

        // Matches the Python sinks: the Kafka key is always preserved on the document.
        doc.put("kafka-key", kafkaKey);

        DocIdStrategy.Result id = idStrategy.compute(doc, kafkaKey);
        if (id.hasId()) {
            doc.put("_id", id.id);
        } else {
            autoIdFallbacks.inc();
            LOG.debug("No deterministic _id (reason={}); inserting with server-generated id", id.reason);
        }

        buffer.add(doc);

        if (buffer.size() >= batchSize
                || System.currentTimeMillis() - lastFlush >= flushIntervalMillis) {
            flushBuffer();
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        flushBuffer();
    }

    private void flushBuffer() throws IOException, InterruptedException {
        if (buffer.isEmpty()) {
            lastFlush = System.currentTimeMillis();
            return;
        }

        List<ObjectNode> pending = new ArrayList<>(buffer);
        buffer.clear();

        for (int attempt = 1; attempt <= maxRetries && !pending.isEmpty(); attempt++) {
            List<CouchDbClient.BulkResult> results = client.bulkDocs(pending);
            List<ObjectNode> retry = new ArrayList<>();

            for (int i = 0; i < results.size(); i++) {
                CouchDbClient.BulkResult r = results.get(i);
                ObjectNode doc = pending.get(i);

                if (r.ok) {
                    documentsWritten.inc();
                } else if (r.isConflict()) {
                    // The document already exists. Re-read its _rev and try once more --
                    // the batched equivalent of the Python's upsert_doc() retry loop.
                    retry.add(doc);
                } else {
                    documentsFailed.inc();
                    String msg = "CouchDB rejected _id=" + doc.path("_id").asText("<auto>")
                            + ": " + r.error + " (" + r.reason + ")";
                    if (!ignoreWriteErrors) {
                        throw new IOException(msg);
                    }
                    LOG.error(msg);
                }
            }

            if (retry.isEmpty()) {
                break;
            }

            if (attempt == maxRetries) {
                documentsFailed.inc(retry.size());
                String msg = "Gave up resolving " + retry.size()
                        + " CouchDB conflict(s) after " + maxRetries + " attempts";
                if (!ignoreWriteErrors) {
                    throw new IOException(msg);
                }
                LOG.error(msg);
                break;
            }

            refreshRevisions(retry);
            conflictsResolved.inc(retry.size());
            pending = retry;

            // Same shape of backoff as the Python: short, linear, bounded by maxRetries.
            Thread.sleep(100L * attempt);
        }

        lastFlush = System.currentTimeMillis();
    }

    /** Re-reads {@code _rev} for each conflicted document so the next attempt is an update. */
    private void refreshRevisions(List<ObjectNode> docs) throws IOException {
        Map<String, String> revs = new HashMap<>();
        for (ObjectNode doc : docs) {
            String id = doc.path("_id").asText(null);
            if (id == null) {
                continue;
            }
            if (!revs.containsKey(id)) {
                revs.put(id, client.currentRev(id));
            }
            String rev = revs.get(id);
            if (rev != null) {
                doc.put("_rev", rev);
            } else {
                // Deleted between the write and the re-read; drop the stale _rev so it inserts.
                doc.remove("_rev");
            }
        }
    }

    @Override
    public void close() throws Exception {
        try {
            flushBuffer();
        } finally {
            client.close();
        }
    }
}
