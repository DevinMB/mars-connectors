package io.mars.connector.couchdb;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.types.RowKind;

/** Table API binding for the CouchDB sink. */
final class CouchDbDynamicTableSink implements DynamicTableSink {

    private final CouchDbConfig config;

    CouchDbDynamicTableSink(CouchDbConfig config) {
        this.config = config;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // Writes are upserts keyed by the computed _id, so an UPDATE_AFTER row needs no
        // special handling. Deletes are never carried out: deleting from CouchDB on a
        // DELETE row would make a replay destructive, which is the opposite of what the
        // deterministic-_id design buys. By default a plan that can produce one is
        // refused. With sink.delete-mode=ignore it is accepted and the row dropped: a
        // join or aggregation retracts a row and writes its replacement under the same
        // _id, so the document ends up right without the delete.
        ChangelogMode.Builder mode = ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER);
        if (config.ignoreDeletes) {
            mode.addContainedKind(RowKind.DELETE);
        }
        return mode.build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkV2Provider.of(new CouchDbSink(config));
    }

    @Override
    public DynamicTableSink copy() {
        return new CouchDbDynamicTableSink(config);
    }

    @Override
    public String asSummaryString() {
        return "CouchDB[" + config.db + ", id.mode=" + config.idMode + "]";
    }
}
