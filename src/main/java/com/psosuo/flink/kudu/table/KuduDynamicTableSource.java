package com.psosuo.flink.kudu.table;

import com.psosuo.flink.kudu.batch.KuduRowDataInputFormat;
import com.psosuo.flink.kudu.batch.KuduRowInputFormat;
import com.psosuo.flink.kudu.connector.KuduFilterInfo;
import com.psosuo.flink.kudu.connector.KuduTableInfo;
import com.psosuo.flink.kudu.connector.reader.KuduReaderConfig;
import com.psosuo.flink.kudu.options.LookupOptions;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.*;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

public class KuduDynamicTableSource implements DynamicTableSource,LookupTableSource ,  SupportsProjectionPushDown,
        SupportsLimitPushDown,ScanTableSource {
    private static final Logger logger =LoggerFactory.getLogger(KuduDynamicTableSource.class);

    private final KuduReaderConfig.Builder configBuilder;
    private final KuduTableInfo tableInfo;
    private  TableSchema flinkSchema;
    private final String[] projectedFields;
    private long limit = -1;

    // predicate expression to apply
    @Nullable
    private final List<KuduFilterInfo> predicates;
    private boolean isFilterPushedDown;

    private KuduRowInputFormat kuduRowInputFormat;

    public KuduDynamicTableSource(KuduReaderConfig.Builder configBuilder, KuduTableInfo tableInfo,
                           TableSchema flinkSchema, List<KuduFilterInfo> predicates, String[] projectedFields) {
        this.configBuilder = configBuilder;
        this.tableInfo = tableInfo;
        this.flinkSchema = flinkSchema;
        this.predicates = predicates;
        this.projectedFields = projectedFields;
        if (predicates != null && predicates.size() != 0) {
            this.isFilterPushedDown = true;
        }
        this.kuduRowInputFormat = new KuduRowInputFormat(configBuilder.build(), tableInfo,
                predicates == null ? Collections.emptyList() : predicates,
                projectedFields == null ? Lists.newArrayList(flinkSchema.getFieldNames()) : Lists.newArrayList(projectedFields));
    }
    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
        LookupOptions build = LookupOptions.builder().build();
        String[] keyNames = new String[lookupContext.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = lookupContext.getKeys()[i];
            Preconditions.checkArgument(innerKeyArr.length == 1,
                    "kudu only support non-nested look up keys");
            keyNames[i] = flinkSchema.getFieldNames()[innerKeyArr[0]];
        }
        KuduDynamicLookupFunction kuduDynamicLookupFunction = new KuduDynamicLookupFunction(build, configBuilder, tableInfo,
                projectedFields == null ? flinkSchema.getFieldNames() : projectedFields, keyNames
        );
        return TableFunctionProvider.of(kuduDynamicLookupFunction);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return  ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        if (limit>-1)
        {
            configBuilder.setRowLimit((int) limit);
        }
        InputFormat inputFormat = new KuduRowDataInputFormat(configBuilder.build(), tableInfo,
                predicates == null ? Collections.emptyList() : predicates,
                projectedFields == null ?  Lists.newArrayList(flinkSchema.getFieldNames()) : Lists.newArrayList(projectedFields));
        return InputFormatProvider.of(inputFormat);
    }

    @Override
    public DynamicTableSource copy() {
        return new KuduDynamicTableSource(configBuilder, tableInfo, flinkSchema, null, projectedFields);
    }

    @Override
    public String asSummaryString() {
        return "KUDU SOURCE";
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
//        return new KuduTableSource(configBuilder.setRowLimit((int) limit), tableInfo, flinkSchema,
//                predicates, projectedFields);
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        this.flinkSchema = TableSchemaUtils.projectSchema(flinkSchema, projectedFields);

    }


}
