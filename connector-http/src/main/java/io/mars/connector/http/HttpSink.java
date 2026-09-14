package io.mars.connector.http;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.table.data.RowData;

/** Sink V2 entry point for the HTTP connector. */
final class HttpSink implements Sink<RowData> {

    private static final long serialVersionUID = 1L;

    private final HttpConfig config;

    HttpSink(HttpConfig config) {
        this.config = config;
    }

    @Override
    public SinkWriter<RowData> createWriter(WriterInitContext context) {
        return new HttpSinkWriter(config, context.metricGroup());
    }
}
