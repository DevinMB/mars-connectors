package io.mars.connector.couchdb;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/** Registers {@code 'connector' = 'couchdb'} with the Flink table planner. */
public final class CouchDbDynamicTableSinkFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "couchdb";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> s = new HashSet<>();
        s.add(CouchDbOptions.URL);
        s.add(CouchDbOptions.DB);
        return s;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> s = new HashSet<>();
        s.add(CouchDbOptions.USERNAME);
        s.add(CouchDbOptions.PASSWORD);
        s.add(CouchDbOptions.ID_MODE);
        s.add(CouchDbOptions.ID_FIELD);
        s.add(CouchDbOptions.ID_TEMPLATE);
        s.add(CouchDbOptions.DOC_COLUMN);
        s.add(CouchDbOptions.KEY_COLUMN);
        s.add(CouchDbOptions.BATCH_SIZE);
        s.add(CouchDbOptions.FLUSH_INTERVAL);
        s.add(CouchDbOptions.MAX_RETRIES);
        s.add(CouchDbOptions.IGNORE_WRITE_ERRORS);
        s.add(CouchDbOptions.HTTP_TIMEOUT);
        return s;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig cfg = helper.getOptions();

        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();

        String docColumn = cfg.get(CouchDbOptions.DOC_COLUMN);
        int docIndex = requireStringColumn(schema, docColumn, true);

        String keyColumn = cfg.get(CouchDbOptions.KEY_COLUMN);
        int keyIndex = requireStringColumn(schema, keyColumn, false);

        String modeRaw = cfg.get(CouchDbOptions.ID_MODE);
        DocIdStrategy.Mode mode = DocIdStrategy.Mode.from(modeRaw);
        if (mode == null) {
            throw new IllegalArgumentException("Unknown '" + CouchDbOptions.ID_MODE.key() + "' value '"
                    + modeRaw + "'. Expected one of: auto, kafka_key, timestamp, timestamp_chat, field, custom.");
        }

        // Catch the two configurations that would silently produce auto ids for every
        // record -- losing idempotency without any error -- at submission time instead.
        if (mode == DocIdStrategy.Mode.FIELD && cfg.get(CouchDbOptions.ID_FIELD).trim().isEmpty()) {
            throw new IllegalArgumentException("'" + CouchDbOptions.ID_MODE.key()
                    + "' = 'field' requires '" + CouchDbOptions.ID_FIELD.key() + "' to be set.");
        }
        if (mode == DocIdStrategy.Mode.KAFKA_KEY && keyIndex < 0) {
            throw new IllegalArgumentException("'" + CouchDbOptions.ID_MODE.key()
                    + "' = 'kafka_key' requires a STRING column named '" + keyColumn
                    + "' (configurable via '" + CouchDbOptions.KEY_COLUMN.key() + "').");
        }

        CouchDbConfig config = new CouchDbConfig(
                cfg.get(CouchDbOptions.URL),
                cfg.get(CouchDbOptions.DB),
                cfg.getOptional(CouchDbOptions.USERNAME).orElse(null),
                cfg.getOptional(CouchDbOptions.PASSWORD).orElse(null),
                mode,
                cfg.get(CouchDbOptions.ID_FIELD),
                cfg.get(CouchDbOptions.ID_TEMPLATE),
                docIndex,
                keyIndex,
                cfg.get(CouchDbOptions.BATCH_SIZE),
                cfg.get(CouchDbOptions.FLUSH_INTERVAL),
                cfg.get(CouchDbOptions.MAX_RETRIES),
                cfg.get(CouchDbOptions.IGNORE_WRITE_ERRORS),
                cfg.get(CouchDbOptions.HTTP_TIMEOUT));

        return new CouchDbDynamicTableSink(config);
    }

    /**
     * Resolves a column by name and asserts it is a character string.
     *
     * @return its index, or -1 when absent and not required
     */
    private static int requireStringColumn(ResolvedSchema schema, String name, boolean required) {
        Optional<Column> col = schema.getColumn(name);
        if (col.isEmpty()) {
            if (required) {
                throw new IllegalArgumentException("Table is missing required column '" + name
                        + "'. The CouchDB sink writes one JSON document per row from this column; "
                        + "rename it or set '" + CouchDbOptions.DOC_COLUMN.key() + "'.");
            }
            return -1;
        }
        LogicalTypeRoot root = col.get().getDataType().getLogicalType().getTypeRoot();
        if (root != LogicalTypeRoot.VARCHAR && root != LogicalTypeRoot.CHAR) {
            throw new IllegalArgumentException("Column '" + name + "' must be STRING but was "
                    + col.get().getDataType() + ".");
        }
        return schema.getColumnNames().indexOf(name);
    }
}
