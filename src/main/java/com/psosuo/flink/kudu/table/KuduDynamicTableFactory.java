package com.psosuo.flink.kudu.table;

import com.psosuo.flink.kudu.connector.KuduTableInfo;
import com.psosuo.flink.kudu.connector.reader.KuduReaderConfig;
import com.psosuo.flink.kudu.connector.writer.KuduWriterConfig;
import com.psosuo.flink.kudu.table.utils.KuduTableUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
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

import static com.psosuo.flink.kudu.table.utils.KuduOptions.*;
import com.psosuo.flink.kudu.options.KuduConnectorOptions;

@MetaInfServices(Factory.class)
@Internal
public class KuduDynamicTableFactory implements  DynamicTableSinkFactory, DynamicTableSourceFactory {
    public static final String IDENTIFIER = "kudu";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();
        //KuduConnectorOptions options = getOptions(config, context.getClassLoader());
        helper.validate();
        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        KuduTableInfo tableInfo = KuduTableUtils.createTableInfo(config.get(KUDU_TABLE), physicalSchema, new HashMap<>());

                ;

        return new KuduDynamicTableSink(getOptions(config), tableInfo, physicalSchema);
    }
        private KuduWriterConfig getOptions(
            ReadableConfig readableConfig) {
            String masterAddresses = readableConfig.get(KUDU_MASTERS);

            KuduWriterConfig.Builder configBuilder = KuduWriterConfig.Builder
                    .setMasters(masterAddresses)
                    .setTimeoutMs(Long.valueOf(readableConfig.get(TIME_OUT_MS)))
                    .setFlushInterval(readableConfig.get(SINK_BUFFER_FLUSH_INTERVAL).getSeconds())
                    .setParallelism(readableConfig.getOptional(SINK_PARALLELISM).orElse(null))
                    .setSinkBufferFlushMaxRows(readableConfig.get(SINK_BUFFER_FLUSH_MAX_ROWS))
                    ;


        return configBuilder.build();
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
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(KUDU_TABLE);
        optionalOptions.add(KUDU_MASTERS);
        optionalOptions.add(SCAN_PARTITION_COLUMN);
        optionalOptions.add(SCAN_PARTITION_LOWER_BOUND);
        optionalOptions.add(SCAN_PARTITION_UPPER_BOUND);
        optionalOptions.add(SCAN_PARTITION_NUM);
        optionalOptions.add(SCAN_FETCH_SIZE);
        optionalOptions.add(SCAN_AUTO_COMMIT);
        optionalOptions.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        optionalOptions.add(SINK_BUFFER_FLUSH_INTERVAL);
        optionalOptions.add(SINK_MAX_RETRIES);
        optionalOptions.add(SINK_PARALLELISM);
        optionalOptions.add(MAX_RETRY_TIMEOUT);
        return optionalOptions;
    }

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
        return new KuduDynamicTableSource(configBuilder,tableInfo,context.getPhysicalRowDataType(),null,null);
    }
}
