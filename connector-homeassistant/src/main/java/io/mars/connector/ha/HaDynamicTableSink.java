package io.mars.connector.ha;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;

/** Table API binding for the Home Assistant sink. */
final class HaDynamicTableSink implements DynamicTableSink {

    private final HaConfig config;

    HaDynamicTableSink(HaConfig config) {
        this.config = config;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // Insert-only: switching a light on cannot be retracted.
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkV2Provider.of(new HaSink(config));
    }

    @Override
    public DynamicTableSink copy() {
        return new HaDynamicTableSink(config);
    }

    @Override
    public String asSummaryString() {
        return "HomeAssistant[" + config.domain + (config.dryRun ? ", dry-run" : "")
                + (config.restorePrevious ? ", restore-previous" : "") + "]";
    }
}
