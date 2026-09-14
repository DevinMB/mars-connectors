package io.mars.connector.http;

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

/** Registers {@code 'connector' = 'http'} with the Flink table planner. */
public final class HttpDynamicTableSinkFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "http";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> s = new HashSet<>();
        s.add(HttpOptions.BASE_URL);
        return s;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> s = new HashSet<>();
        s.add(HttpOptions.METHOD);
        s.add(HttpOptions.AUTH_HEADER);
        s.add(HttpOptions.AUTH_VALUE);
        s.add(HttpOptions.CONTENT_TYPE);
        s.add(HttpOptions.PATH_COLUMN);
        s.add(HttpOptions.BODY_COLUMN);
        s.add(HttpOptions.METHOD_COLUMN);
        s.add(HttpOptions.MAX_RETRIES);
        s.add(HttpOptions.TIMEOUT);
        s.add(HttpOptions.DRY_RUN);
        s.add(HttpOptions.IGNORE_ERRORS);
        return s;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig cfg = helper.getOptions();
        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();

        int pathIndex = stringColumn(schema, cfg.get(HttpOptions.PATH_COLUMN), true);
        int bodyIndex = stringColumn(schema, cfg.get(HttpOptions.BODY_COLUMN), false);
        int methodIndex = stringColumn(schema, cfg.get(HttpOptions.METHOD_COLUMN), false);

        String baseUrl = cfg.get(HttpOptions.BASE_URL);
        if (baseUrl.endsWith("/")) {
            baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
        }

        HttpConfig config = new HttpConfig(
                baseUrl,
                cfg.get(HttpOptions.METHOD).toUpperCase(),
                cfg.get(HttpOptions.AUTH_HEADER),
                cfg.getOptional(HttpOptions.AUTH_VALUE).orElse(null),
                cfg.get(HttpOptions.CONTENT_TYPE),
                pathIndex, bodyIndex, methodIndex,
                cfg.get(HttpOptions.MAX_RETRIES),
                cfg.get(HttpOptions.TIMEOUT),
                cfg.get(HttpOptions.DRY_RUN),
                cfg.get(HttpOptions.IGNORE_ERRORS));

        return new HttpDynamicTableSink(config);
    }

    private static int stringColumn(ResolvedSchema schema, String name, boolean required) {
        Optional<Column> col = schema.getColumn(name);
        if (col.isEmpty()) {
            if (required) {
                throw new IllegalArgumentException("Table is missing required column '" + name
                        + "'. The HTTP sink sends one request per row using this column as the "
                        + "path; rename it or set '" + HttpOptions.PATH_COLUMN.key() + "'.");
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
