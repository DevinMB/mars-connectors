package io.mars.connector.ha;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.table.data.RowData;

/** Sink V2 entry point for the Home Assistant connector. */
final class HaSink implements Sink<RowData> {

    private static final long serialVersionUID = 1L;

    private final HaConfig config;

    HaSink(HaConfig config) {
        this.config = config;
    }

    @Override
    public SinkWriter<RowData> createWriter(WriterInitContext context) {
        return new HaSinkWriter(config, context.metricGroup());
    }
}
