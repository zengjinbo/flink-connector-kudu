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
package com.psosuo.flink.kudu.table;

import com.psosuo.flink.kudu.connector.writer.AbstractSingleOperationMapper;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Optional;

@Internal
public class UpsertDynamicOperationMapper extends AbstractSingleOperationMapper<RowData> {
    private final DataType[] fieldDataType;
    public UpsertDynamicOperationMapper( TableSchema flinkSchema) {
        super(flinkSchema.getFieldNames());
      fieldDataType=  flinkSchema.getFieldDataTypes();
    }

    @Override
    public PartialRow getField(RowData input,String c, int i,PartialRow partialRow)  {
        try {
            if (input.isNullAt(i))
                partialRow.setNull(c);
            else
            createExternalConverter(fieldDataType[i].getLogicalType()).serialize(input,c,i,partialRow);

        }catch (Exception r)
        {
            r.printStackTrace();
        }
        return partialRow ;
    }

    @Override
    public Object getField(RowData input, int i) {
        return null;
    }

    @Override
    public Optional<Operation> createBaseOperation(RowData input, KuduTable table) {
        switch (input.getRowKind())
        {
            case DELETE:
                return Optional.of(table.newDelete());
            default:
                return Optional.of(table.newUpsert());
        }

    }

    @FunctionalInterface
    interface SerializationConverter extends Serializable {
        void serialize(RowData rowData,String cl, int index, PartialRow partialRow) ;
    }
    protected SerializationConverter createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (val,cl, index, statement) -> statement.addBoolean(cl, val.getBoolean(index));
            case TINYINT:
                return (val,cl, index, statement) -> statement.addByte(cl, val.getByte(index));
            case SMALLINT:
                return (val,cl, index, statement) -> statement.addShort(cl, val.getShort(index));
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return (val, cl,index, statement) -> statement.addInt(cl, val.getInt(index));
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (val, cl,index, statement) -> statement.addLong(cl, val.getLong(index));
            case FLOAT:
                return (val, cl,index, statement) -> statement.addFloat(cl, val.getFloat(index));
            case DOUBLE:
                return (val, cl,index, statement) -> statement.addDouble(cl, val.getDouble(index));
            case CHAR:
            case VARCHAR:
                // value is BinaryString
                return (val, cl,index, statement) -> statement.addString(cl, val.getString(index).toString());
            case BINARY:
            case VARBINARY:
                return (val, cl,index, statement) -> statement.addObject(cl, val.getBinary(index));
            case DATE:
                return (val, cl,index, statement) ->
                        statement.addDate(cl, Date.valueOf(LocalDate.ofEpochDay(val.getInt(index))));
            case TIME_WITHOUT_TIME_ZONE:
                return (val, cl,index, statement) ->
                        statement.addObject(cl, Time.valueOf(LocalTime.ofNanoOfDay(val.getInt(index) * 1_000_000L)));
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                return (val, cl,index, statement) ->
                        statement.addTimestamp(
                                cl,
                                val.getTimestamp(index, timestampPrecision).toTimestamp());
            case DECIMAL:
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return (val, cl,index, statement) ->
                        statement.addDecimal(
                                cl,
                                val.getDecimal(index, decimalPrecision, decimalScale).toBigDecimal());
            case ARRAY:
            case MAP:
            case MULTISET:
            case ROW:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
