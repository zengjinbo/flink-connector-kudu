package com.psosuo.flink.kudu.table;

import com.psosuo.flink.kudu.batch.KuduOutputFormat;
import com.psosuo.flink.kudu.connector.KuduTableInfo;
import com.psosuo.flink.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import static org.apache.flink.util.Preconditions.checkState;

public class KuduDynamicTableSink implements DynamicTableSink {
    private final KuduWriterConfig.Builder writerConfigBuilder;
    private final transient TableSchema flinkSchema;
    private final transient KuduTableInfo tableInfo;

    public KuduDynamicTableSink(KuduWriterConfig.Builder configBuilder, KuduTableInfo tableInfo, TableSchema flinkSchema) {
        this.writerConfigBuilder = configBuilder;
        this.tableInfo = tableInfo;
        this.flinkSchema = flinkSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        validatePrimaryKey(changelogMode);
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    private void validatePrimaryKey(ChangelogMode requestedMode) {
        checkState(ChangelogMode.insertOnly().equals(requestedMode) || flinkSchema.getPrimaryKey().isPresent(),
                "please declare primary key for sink table when query contains update/delete record.");
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        KuduOutputFormat<RowData> kuduOutputFormat =new KuduOutputFormat<>(writerConfigBuilder.build(),tableInfo, new UpsertDynamicOperationMapper(flinkSchema));
        return  OutputFormatProvider.of(kuduOutputFormat);
    }

    @Override
    public DynamicTableSink copy() {
        return new KuduDynamicTableSink(writerConfigBuilder,tableInfo,flinkSchema);
    }

    @Override
    public String asSummaryString() {
         return "KUDU SINK" ;
    }
}
