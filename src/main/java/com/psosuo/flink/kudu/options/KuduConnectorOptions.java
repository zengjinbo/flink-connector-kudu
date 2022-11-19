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

package com.psosuo.flink.kudu.options;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.KuduConnectionOptions;


import javax.annotation.Nullable;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Options for the JDBC connector. */
@Internal
public class KuduConnectorOptions extends KuduConnectionOptions {

    private static final long serialVersionUID = 1L;

    private final String tableName;
    private final @Nullable Integer parallelism;

    private KuduConnectorOptions(
            String masters,
            String tableName,
            @Nullable Integer parallelism,
            int connectionCheckTimeoutSeconds) {
        super(masters, connectionCheckTimeoutSeconds);
        this.tableName = tableName;
        this.parallelism = parallelism;
    }

    public String getTableName() {
        return tableName;
    }


    public Integer getParallelism() {
        return parallelism;
    }

    public static Builder builder() {
        return new Builder();
    }




    @Override
    public int hashCode() {
        return Objects.hash(
                masters,
                tableName,
                parallelism
                );
    }

    public static class Builder {
        private ClassLoader classLoader;
        private String masters;
        private String tableName;

        private Integer parallelism;
        private int connectionCheckTimeoutSeconds = 60;

        /**
         * optional, specifies the classloader to use in the planner for load the class in user jar.
         *
         * <p>By default, this is configured using {@code
         * Thread.currentThread().getContextClassLoader()}.
         *
         * <p>Modify the {@link ClassLoader} only if you know what you're doing.
         */
        public Builder setClassLoader(ClassLoader classLoader) {
            this.classLoader = classLoader;
            return this;
        }

        /** required, table name. */
        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }



        /** optional, connectionCheckTimeoutSeconds. */
        public Builder setConnectionCheckTimeoutSeconds(int connectionCheckTimeoutSeconds) {
            this.connectionCheckTimeoutSeconds = connectionCheckTimeoutSeconds;
            return this;
        }



        /** required, JDBC DB url. */
        public Builder setMasters(String masters) {
            this.masters = masters;
            return this;
        }


        public Builder setParallelism(Integer parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public KuduConnectorOptions build() {
            checkNotNull(masters, "No masters supplied.");
            checkNotNull(tableName, "No tableName supplied.");


            return new KuduConnectorOptions(
                    masters,
                    tableName,
                    parallelism,
                    connectionCheckTimeoutSeconds);
        }
    }
}
