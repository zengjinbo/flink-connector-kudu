package com.psosuo.flink.kudu.table;

import com.psosuo.flink.kudu.connector.KuduTableInfo;
import com.psosuo.flink.kudu.connector.reader.KuduReaderConfig;
import com.psosuo.flink.kudu.connector.writer.KuduWriterConfig;
import com.psosuo.flink.kudu.table.utils.KuduTableUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.*;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.kohsuke.MetaInfServices;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
@MetaInfServices(Factory.class)
@Internal
public class KuduDynamicTableFactory implements  DynamicTableSinkFactory, DynamicTableSourceFactory {
    public static final String IDENTIFIER = "kudu";
    public static final ConfigOption<String> KUDU_MASTERS = ConfigOptions
            .key("masters")
            .stringType()
            .noDefaultValue()
            .withDescription("the jdbc  masters.");
    public static final ConfigOption<String> KUDU_TABLE = ConfigOptions
            .key("table")
            .stringType()
            .noDefaultValue()
            .withDescription("the jdbc  masters.");
    public static final ConfigOption<Long> TIME_OUT_MS = ConfigOptions
            .key("timeout.ms")
            .longType()
            .defaultValue(1800*1000l)
            .withDescription("the operation timeout value(ms).");
    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();
        helper.validate();
        String masterAddresses = config.get(KUDU_MASTERS);
        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        KuduTableInfo tableInfo = KuduTableUtils.createTableInfo(config.get(KUDU_TABLE), physicalSchema, new HashMap<>());
        KuduWriterConfig.Builder configBuilder = KuduWriterConfig.Builder
                .setMasters(masterAddresses)
                .setTimeoutMs(Long.valueOf(config.get(TIME_OUT_MS)))
                ;

        return new KuduDynamicTableSink(configBuilder, tableInfo, physicalSchema);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(KUDU_TABLE);
        requiredOptions.add(KUDU_MASTERS);
        return requiredOptions;    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(KUDU_TABLE);
        requiredOptions.add(KUDU_MASTERS);
        return requiredOptions;    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();
        helper.validate();
        String masterAddresses = config.get(KUDU_MASTERS);
        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        KuduTableInfo tableInfo = KuduTableUtils.createTableInfo(config.get(KUDU_TABLE), physicalSchema, new HashMap<>());

        KuduReaderConfig.Builder configBuilder = KuduReaderConfig.Builder
                .setMasters(masterAddresses);
        return new KuduDynamicTableSource(configBuilder,tableInfo,physicalSchema,null,null);
    }
}
