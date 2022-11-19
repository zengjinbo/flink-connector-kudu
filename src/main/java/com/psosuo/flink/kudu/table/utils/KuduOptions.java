/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.psosuo.flink.kudu.table.utils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.Duration;

/** Options for the JDBC connector. */
@PublicEvolving
public class KuduOptions {

    public static final ConfigOption<String> KUDU_MASTERS =
            ConfigOptions.key("masters")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The kudu URL.");

    public static final ConfigOption<String> KUDU_TABLE =
            ConfigOptions.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The kudu table name.");





    public static final ConfigOption<Duration> MAX_RETRY_TIMEOUT =
            ConfigOptions.key("connection.max-retry-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60))
                    .withDescription("Maximum timeout between retries.");

    public static final ConfigOption<Integer> SINK_PARALLELISM = FactoryUtil.SINK_PARALLELISM;

    // -----------------------------------------------------------------------------------------
    // Scan options
    // -----------------------------------------------------------------------------------------

    public static final ConfigOption<String> SCAN_PARTITION_COLUMN =
            ConfigOptions.key("scan.partition.column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The column name used for partitioning the input.");

    public static final ConfigOption<Integer> SCAN_PARTITION_NUM =
            ConfigOptions.key("scan.partition.num")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The number of partitions.");

    public static final ConfigOption<Long> SCAN_PARTITION_LOWER_BOUND =
            ConfigOptions.key("scan.partition.lower-bound")
                    .longType()
                    .noDefaultValue()
                    .withDescription("The smallest value of the first partition.");

    public static final ConfigOption<Long> SCAN_PARTITION_UPPER_BOUND =
            ConfigOptions.key("scan.partition.upper-bound")
                    .longType()
                    .noDefaultValue()
                    .withDescription("The largest value of the last partition.");

    public static final ConfigOption<Integer> SCAN_FETCH_SIZE =
            ConfigOptions.key("scan.fetch-size")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "Gives the reader a hint as to the number of rows that should be fetched "
                                    + "from the database per round-trip when reading. "
                                    + "If the value is zero, this hint is ignored.");

    public static final ConfigOption<Boolean> SCAN_AUTO_COMMIT =
            ConfigOptions.key("scan.auto-commit")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Sets whether the driver is in auto-commit mode.");

    // -----------------------------------------------------------------------------------------
    // Lookup options
    // -----------------------------------------------------------------------------------------




    // write config options
    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS =
            ConfigOptions.key("sink.buffer-flush.max-rows")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "The flush max size (includes all append, upsert and delete records), over this number"
                                    + " of records, will flush data.");

    public static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL =
            ConfigOptions.key("sink.buffer-flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription(
                            "The flush interval mills, over this time, asynchronous threads will flush data.");
        public static final ConfigOption<Long> TIME_OUT_MS = ConfigOptions
            .key("timeout.ms")
            .longType()
            .defaultValue(1800*1000l)
            .withDescription("the operation timeout value(ms).");
    public static final ConfigOption<Integer> SINK_MAX_RETRIES =
            ConfigOptions.key("sink.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("The max retry times if writing records to database failed.");

    private KuduOptions() {}

}
