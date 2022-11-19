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

import com.psosuo.flink.kudu.batch.KuduOutputFormat;
import com.psosuo.flink.kudu.options.KuduConnectorOptions;
import com.psosuo.flink.kudu.streaming.KuduSink;
import org.apache.flink.annotation.PublicEvolving;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.kudu.client.SessionConfiguration.FlushMode;

/**
 * Configuration used by {@link KuduSink} and {@link KuduOutputFormat}.
 * Specifies connection and other necessary properties.
 */
@PublicEvolving
public class KuduWriterConfig implements Serializable {

    private final String masters;
    private final FlushMode flushMode;
    private final Long timeoutMs;
    private final Long sinkBufferFlushMaxRows;
    private final Long flushInterval;
    private final Integer parallelism;
    private KuduWriterConfig(
            String masters,
            FlushMode flushMode,
            Long timeoutMs,Long sinkBufferFlushMaxRows,Long flushInterval,Integer parallelism) {

        this.masters = checkNotNull(masters, "Kudu masters cannot be null");
        this.flushMode = checkNotNull(flushMode, "Kudu flush mode cannot be null");
        this.timeoutMs = timeoutMs;
        this.sinkBufferFlushMaxRows=sinkBufferFlushMaxRows;
        this.flushInterval=flushInterval;
        this.parallelism=parallelism;

    }

    public String getMasters() {
        return masters;
    }

    public FlushMode getFlushMode() {
        return flushMode;
    }

    public Long getTimeoutMs() {
        return timeoutMs;
    }

    public Long getSinkBufferFlushMaxRows() {
        return sinkBufferFlushMaxRows;
    }
    public Integer getParallelism() {
        return parallelism;
    }
    public Long getFlushInterval() {
        return flushInterval;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("masters", masters)
                .append("flushMode", flushMode)
                .append("timeoutMs", timeoutMs)
                .toString();
    }

    /**
     * Builder for the {@link KuduWriterConfig}.
     */
    public static class Builder {
        private String masters;
        private FlushMode flushMode = FlushMode.AUTO_FLUSH_BACKGROUND;
        private Long timeoutMs;
        private Integer parallelism;
        private  Long sinkBufferFlushMaxRows;
        private  Long flushInterval;

        private Builder(String masters) {
            this.masters = masters;
        }

        public static Builder setMasters(String masters) {
            return new Builder(masters);
        }

        public Builder setConsistency(FlushMode flushMode) {
            this.flushMode = flushMode;
            return this;
        }

        public Builder setTimeoutMs(Long timeoutMs) {
            this.timeoutMs = timeoutMs;
            return this;
        }
        public Builder setParallelism(Integer parallelism) {
            this.parallelism = parallelism;
            return this;
        }
        public Builder setSinkBufferFlushMaxRows(long sinkBufferFlushMaxRows) {
            this.sinkBufferFlushMaxRows = sinkBufferFlushMaxRows;
            return this;
        }

        public Builder setFlushInterval(Long flushInterval) {
            this.flushInterval = flushInterval;
            return this;
        }

        public Builder setEventualConsistency() {
            return setConsistency(FlushMode.AUTO_FLUSH_BACKGROUND);
        }

        public Builder setStrongConsistency() {
            return setConsistency(FlushMode.AUTO_FLUSH_SYNC);
        }

        public KuduWriterConfig build() {
            return new KuduWriterConfig(
                    masters,
                    flushMode,
                    timeoutMs,sinkBufferFlushMaxRows,flushInterval,parallelism);
        }
    }
}
