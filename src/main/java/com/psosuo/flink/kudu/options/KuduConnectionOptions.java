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

package org.apache.flink.connector.jdbc;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Optional;

/** kudu connection options. */
@PublicEvolving
public class KuduConnectionOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final String masters;

    protected final int connectionCheckTimeoutSeconds;


    protected KuduConnectionOptions(
            String masters,

            int connectionCheckTimeoutSeconds) {
        Preconditions.checkArgument(connectionCheckTimeoutSeconds > 0);
        this.masters = Preconditions.checkNotNull(masters, "masters url is empty");
        this.connectionCheckTimeoutSeconds = connectionCheckTimeoutSeconds;
    }

    public String getMasters() {
        return masters;
    }


    public int getConnectionCheckTimeoutSeconds() {
        return connectionCheckTimeoutSeconds;
    }

    /** Builder for {@link JdbcConnectionOptions}. */
    public static class  KuduConnectionOptionsBuilder {
        private String masters;

        private int connectionCheckTimeoutSeconds = 60;

        public KuduConnectionOptionsBuilder withMasters(String masters) {
            this.masters = masters;
            return this;
        }



        /**
         * Set the maximum timeout between retries, default is 60 seconds.
         *
         * @param connectionCheckTimeoutSeconds the timeout seconds, shouldn't smaller than 1
         *     second.
         */
        public KuduConnectionOptionsBuilder withConnectionCheckTimeoutSeconds(
                int connectionCheckTimeoutSeconds) {
            this.connectionCheckTimeoutSeconds = connectionCheckTimeoutSeconds;
            return this;
        }

        public KuduConnectionOptions build() {
            return new KuduConnectionOptions(
                    masters,  connectionCheckTimeoutSeconds);
        }
    }
}
