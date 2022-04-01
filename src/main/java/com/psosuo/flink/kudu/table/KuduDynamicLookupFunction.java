package com.psosuo.flink.kudu.table;

import com.psosuo.flink.kudu.connector.KuduFilterInfo;
import com.psosuo.flink.kudu.connector.KuduTableInfo;
import com.psosuo.flink.kudu.connector.reader.KuduReaderConfig;
import com.psosuo.flink.kudu.connector.reader.KuduRowDataReaderIterator;
import com.psosuo.flink.kudu.options.LookupOptions;
import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class KuduDynamicLookupFunction extends TableFunction<RowData> {
    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;
    private final KuduTableInfo tableInfo;
    private final String[] fieldNames;
    private final String[] keyNames;
    private transient ColumnSchema[] columnSchemas;
    private transient Cache<RowData, List<RowData>> cache;
    private transient KuduClient client;
    private transient KuduTable table;
    private final KuduReaderConfig configBuilder;

    public KuduDynamicLookupFunction(LookupOptions lookupOptions, KuduReaderConfig.Builder configBuilder, KuduTableInfo tableInfo, String[] fieldNames, String[] keyNames) {
        this.cacheMaxSize = lookupOptions.getCacheMaxSize();
        this.cacheExpireMs = lookupOptions.getCacheExpireMs();
        this.maxRetryTimes = lookupOptions.getMaxRetryTimes();
        this.keyNames = keyNames;
        this.fieldNames = fieldNames;
        this.configBuilder = configBuilder.build();
        this.tableInfo = tableInfo;
    }

    private static final Logger logger = LoggerFactory.getLogger(KuduDynamicLookupFunction.class);

    @Override
    public void open(FunctionContext context) throws Exception {

        this.cache = cacheMaxSize == -1 || cacheExpireMs == -1 ? null : CacheBuilder.newBuilder()
                .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                .maximumSize(cacheMaxSize)
                .build();
        this.client = obtainClient();
        this.table = obtainTable();
        this.columnSchemas = obtainColumnSchema();
        //Arrays.asList(fieldNames).f
    }

    public void eval(Object... keys) throws IOException {
        RowData keyRow = GenericRowData.of(keys);
        if (cache != null) {
            List<RowData> cachedRows = cache.getIfPresent(keyRow);
            if (cachedRows != null) {
                cachedRows.forEach(cachedRow -> {
                    collect(cachedRow);
                });
                return;
            }
        }
        for (int retry = 1; retry <= maxRetryTimes; retry++) {
            try {
                KuduScanner.KuduScannerBuilder kuduScannerBuilder = client.newScannerBuilder(table);
                if (fieldNames != null) {
                    kuduScannerBuilder.setProjectedColumnNames(Lists.newArrayList(fieldNames));
                }

                for (int i = 0; i < keys.length; i++) {
                    KuduFilterInfo.Builder builder = KuduFilterInfo.Builder.create(columnSchemas[i].getName());
                    builder.equalTo(dataTypeChage(keys[i]));
                    KuduFilterInfo kuduFilterInfo = builder.build();
                    kuduScannerBuilder.addPredicate(kuduFilterInfo.toPredicate(columnSchemas[i]));
                }
                KuduRowDataReaderIterator iterator = new KuduRowDataReaderIterator(kuduScannerBuilder.build());
                ArrayList<RowData> rows = new ArrayList<>();

                while (iterator.hasNext()) {
                    RowData rowData = iterator.next();

                    try {
                        collect(rowData);

                    } catch (Exception e) {
                        logger.error("collect :", e);
                    }
                    rows.add(rowData);
                }
//                if (rows.size()<=0)
//                    collect( toInternal(keys));

                if (cache != null)
                    cache.put(keyRow, rows);

            } catch (Exception e) {
                logger.error(String.format("kudu executeBatch error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException("Execution of kudu statement failed." + keys, e);
                }


                try {
                    Thread.sleep(1000 * retry);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        }
    }

    public Object dataTypeChage(Object object) {
        if (object instanceof BigInteger)
            return ((BigInteger) object).longValue();
//        else if (object instanceof BigDecimal)
//            return ((BigDecimal) object).doubleValue();
        else if (object instanceof BinaryStringData)
            return object.toString();
        else
            return object;
    }

    private KuduClient obtainClient() {
        return new KuduClient.KuduClientBuilder(this.configBuilder.getMasters()).build();
    }

    private KuduTable obtainTable() throws IOException {
        String tableName = tableInfo.getName();
        if (client.tableExists(tableName)) {
            return client.openTable(tableName);
        }
        if (tableInfo.getCreateTableIfNotExists()) {
            return client.createTable(tableName, tableInfo.getSchema(), tableInfo.getCreateTableOptions());
        }
        throw new UnsupportedOperationException("table not exists and is marketed to not be created");
    }

    private ColumnSchema[] obtainColumnSchema() throws IOException {
        ColumnSchema[] list = new ColumnSchema[keyNames.length];
        Schema tableSchema = table.getSchema();
        for (int i = 0; i < keyNames.length; i++) {
            if (tableSchema.hasColumn(keyNames[i])) {
                list[i] = tableSchema.getColumn(keyNames[i]);
            } else {
                int finalI = i;
                tableSchema.getColumns().forEach(c -> {
                    if (c.getName().equalsIgnoreCase(keyNames[finalI])) {
                        list[finalI] = c;
                    }
                });
            }
        }
        return list;
    }
}
