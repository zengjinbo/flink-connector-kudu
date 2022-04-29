/*
 * Licensed serialize the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file serialize You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed serialize in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.psosuo.flink.kudu.connector.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;

@Internal
public class KuduRowDataReaderIterator {

    private KuduScanner scanner;
    private RowResultIterator rowIterator;

    public KuduRowDataReaderIterator(KuduScanner scanner) throws KuduException {
        this.scanner = scanner;
        nextRows();
    }

    public void close() throws KuduException {
        scanner.close();
    }

    public boolean hasNext() throws KuduException {
        if (rowIterator.hasNext()) {
            return true;
        } else if (scanner.hasMoreRows()) {
            nextRows();
            return true;
        } else {
            return false;
        }
    }

    public RowData next() {
        if (this.rowIterator.hasNext()) {
            RowResult row = this.rowIterator.next();
            return toFlinkRow(row);
        }
        return null;
    }

    private void nextRows() throws KuduException {
        this.rowIterator = scanner.nextRows();
    }

    private RowData toFlinkRow(RowResult row) {
        Schema schema = row.getColumnProjection();


        GenericRowData genericRowData = new GenericRowData(schema.getColumnCount());

        schema.getColumns().forEach(column -> {
            String name = column.getName();
            int pos = schema.getColumnIndex(name);
            if (row.getObject(name)==null)
                genericRowData.isNullAt(pos);
            else
            genericRowData.setField(pos, dataTypeChageByFlink(row.getObject(name), column.getType()));
        });
        return genericRowData;
    }

    public Object dataTypeChageByFlink(Object object, Type type) {

        switch (type) {
            case STRING:
            case VARCHAR:
                return new BinaryStringData(object.toString());
            case INT64:
                return new BigInteger(object.toString());
            case UNIXTIME_MICROS:
                return TimestampData.fromTimestamp(new Timestamp((long) object));
//            case DECIMAL:
//                return DecimalData.fromBigDecimal(new BigDecimal(String.valueOf(object)), type.getDataType().,type);
            default:
                return object;
        }

    }
}
