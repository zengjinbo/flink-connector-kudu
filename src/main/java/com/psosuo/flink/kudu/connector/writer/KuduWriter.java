/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.psosuo.flink.kudu.connector.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import com.psosuo.flink.kudu.connector.KuduTableInfo;
import com.psosuo.flink.kudu.connector.failure.DefaultKuduFailureHandler;
import com.psosuo.flink.kudu.connector.failure.KuduFailureHandler;

import org.apache.kudu.client.*;
import org.slf4j.Logger;import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Internal
public class KuduWriter<T> implements AutoCloseable {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final KuduTableInfo tableInfo;
    private final KuduWriterConfig writerConfig;
    private final KuduFailureHandler failureHandler;
    private final KuduOperationMapper<T> operationMapper;

    private transient KuduClient client;
    private transient KuduSession session;
    private transient KuduTable table;

    public KuduWriter(KuduTableInfo tableInfo, KuduWriterConfig writerConfig, KuduOperationMapper<T> operationMapper) throws IOException {
        this(tableInfo, writerConfig, operationMapper, new DefaultKuduFailureHandler());
    }

    public KuduWriter(KuduTableInfo tableInfo, KuduWriterConfig writerConfig, KuduOperationMapper<T> operationMapper, KuduFailureHandler failureHandler) throws IOException {
        this.tableInfo = tableInfo;
        this.writerConfig = writerConfig;
        this.failureHandler = failureHandler;

        this.client = obtainClient();
        this.session = obtainSession();
        this.table = obtainTable();
        this.operationMapper = operationMapper;
    }

    private KuduClient obtainClient() {
        return new KuduClient.KuduClientBuilder(writerConfig.getMasters())
                .defaultOperationTimeoutMs(writerConfig.getTimeoutMs())
                .build();
    }

    private KuduSession obtainSession() {
        KuduSession session = client.newSession();
        session.setFlushMode(writerConfig.getFlushMode());
        session.setFlushInterval(writerConfig.getFlushInterval().intValue());
        session.setMutationBufferSpace(writerConfig.getSinkBufferFlushMaxRows().intValue());
        return session;
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

    public void write(T input) throws IOException {
        checkAsyncErrors();
        for (Operation operation : operationMapper.createOperations(input, table)) {
            try {
                checkErrors(session.apply(operation));
            }catch (org.apache.kudu.client.KuduException e){
                //当前buffer已满
                if(e.getMessage().equals("MANUAL_FLUSH is enabled but the buffer is too big")){
                    session.flush();
                    checkErrors(session.apply(operation));
                }else {
                    throw e;
                }

            }

        }
    }

    public void flushAndCheckErrors() throws IOException {
        checkAsyncErrors();
        flush();
        checkAsyncErrors();
    }

    @VisibleForTesting
    public DeleteTableResponse deleteTable() throws IOException {
        String tableName = table.getName();
        return client.deleteTable(tableName);
    }

    @Override
    public  void  close() throws IOException {
        try {
            flushAndCheckErrors();
        } finally {
            try {
                if (session != null) {
                    session.close();
                }
            } catch (Exception e) {
                log.error("Error while closing session.", e);
            }
            try {
                if (client != null) {
                    client.close();
                }
            } catch (Exception e) {
                log.error("Error while closing client.", e);
            }
        }
    }

    private void flush() throws IOException {
        List<OperationResponse> responses = session.flush();
        for (OperationResponse response : responses) {
            if (!response.hasRowError()) {
                continue;
            }
            String errorString = response.getRowError().toString();
            // 主要过滤kudu 删除的时候，删除到了不存在的数据
            if(errorString.contains("key not found")){
                log.warn("encounter key not found error.More.detail " +
                        "=> table : "+ response.getRowError().getOperation().getTable().getName() +"" +
                        "=> row : "+response.getRowError().getOperation().getRow().stringifyRowKey());
                continue;
            }
            if (response.hasRowError()) {
                throw new RuntimeException("encounter key not found error.More.detail " +
                        "=> table : "+ response.getRowError().getOperation().getTable().getName() +"" +
                        "=> row : "+response.getRowError().getOperation().getRow().stringifyRowKey());
            }
        }

    }

    private void checkErrors(OperationResponse response) throws IOException {
        if (response != null && response.hasRowError()) {
            failureHandler.onFailure(Arrays.asList(response.getRowError()));
        } else {
            checkAsyncErrors();
        }
    }

    private void checkAsyncErrors() throws IOException {
        if (session.countPendingErrors() == 0) { return; }

        RowError[] rowErrors = session.getPendingErrors().getRowErrors();
        List<RowError> errors = new ArrayList<>();
        for(RowError err : rowErrors){
            if(!err.toString().contains("key not found")){//过滤删除不存在的数据的错误
                errors.add(err);
            }
        }
       // List<RowError> errors = Arrays.asList(session.getPendingErrors().getRowErrors());
        if(errors.size()!=0){
            failureHandler.onFailure(errors);
        }

    }
}
