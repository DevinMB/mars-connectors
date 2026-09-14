package io.mars.connector.http;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;

/** Table API binding for the HTTP sink. */
final class HttpDynamicTableSink implements DynamicTableSink {

    private final HttpConfig config;

    HttpDynamicTableSink(HttpConfig config) {
        this.config = config;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // Insert-only. An HTTP call cannot be retracted, so accepting updates or
        // deletes would imply an undo this sink cannot perform.
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkV2Provider.of(new HttpSink(config));
    }

    @Override
    public DynamicTableSink copy() {
        return new HttpDynamicTableSink(config);
    }

    @Override
    public String asSummaryString() {
        return "HTTP[" + config.baseUrl + (config.dryRun ? ", dry-run" : "") + "]";
    }
}
