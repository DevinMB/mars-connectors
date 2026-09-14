package io.mars.connector.ha;

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

/** Registers {@code 'connector' = 'home-assistant'} with the Flink table planner. */
public final class HaDynamicTableSinkFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "home-assistant";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> s = new HashSet<>();
        s.add(HaOptions.BASE_URL);
        s.add(HaOptions.TOKEN);
        return s;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> s = new HashSet<>();
        s.add(HaOptions.DOMAIN);
        s.add(HaOptions.ACTION_COLUMN);
        s.add(HaOptions.ENTITY_COLUMN);
        s.add(HaOptions.ATTRIBUTES_COLUMN);
        s.add(HaOptions.RESTORE_PREVIOUS);
        s.add(HaOptions.DRY_RUN);
        s.add(HaOptions.IGNORE_ERRORS);
        s.add(HaOptions.TIMEOUT);
        return s;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig cfg = helper.getOptions();
        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();

        String baseUrl = cfg.get(HaOptions.BASE_URL);
        if (baseUrl.endsWith("/")) {
            baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
        }

        HaConfig config = new HaConfig(
                baseUrl,
                cfg.get(HaOptions.TOKEN),
                cfg.get(HaOptions.DOMAIN),
                stringColumn(schema, cfg.get(HaOptions.ACTION_COLUMN), true),
                stringColumn(schema, cfg.get(HaOptions.ENTITY_COLUMN), true),
                stringColumn(schema, cfg.get(HaOptions.ATTRIBUTES_COLUMN), false),
                cfg.get(HaOptions.RESTORE_PREVIOUS),
                cfg.get(HaOptions.DRY_RUN),
                cfg.get(HaOptions.IGNORE_ERRORS),
                cfg.get(HaOptions.TIMEOUT));

        return new HaDynamicTableSink(config);
    }

    private static int stringColumn(ResolvedSchema schema, String name, boolean required) {
        Optional<Column> col = schema.getColumn(name);
        if (col.isEmpty()) {
            if (required) {
                throw new IllegalArgumentException("Table is missing required column '" + name
                        + "'. The Home Assistant sink needs it to know what to do and to what.");
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
