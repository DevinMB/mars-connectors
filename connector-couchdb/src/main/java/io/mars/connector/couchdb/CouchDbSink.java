package io.mars.connector.couchdb;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

/** Sink V2 entry point for the CouchDB connector. */
final class CouchDbSink implements Sink<RowData> {

    private static final long serialVersionUID = 1L;

    private final CouchDbConfig config;

    CouchDbSink(CouchDbConfig config) {
        this.config = config;
    }

    @Override
    public SinkWriter<RowData> createWriter(WriterInitContext context) throws IOException {
        return new CouchDbSinkWriter(config, context.metricGroup());
    }
}
