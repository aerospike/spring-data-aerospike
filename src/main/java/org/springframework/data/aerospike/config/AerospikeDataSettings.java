/*
 * Copyright 2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.core.env.Environment;

import static org.springframework.data.aerospike.config.AerospikeDataConfigurationSupport.CONFIG_PREFIX_DATA;
import static org.springframework.data.aerospike.util.Utils.setBoolFromConfig;
import static org.springframework.data.aerospike.util.Utils.setIntFromConfig;
import static org.springframework.data.aerospike.util.Utils.setStringFromConfig;

@Setter
@Getter
public class AerospikeDataSettings {

    // Namespace
    String namespace = "test";
    // Enable scan operation
    boolean scansEnabled = false;
    // Create secondary indexes specified using `@Indexed` annotation on startup
    boolean createIndexesOnStartup = true;
    // Automatically refresh indexes cache every <N> seconds
    int indexCacheRefreshSeconds = 3600;
    // Automatically refresh cached server version every <N> seconds
    int serverVersionRefreshSeconds = 3600;
    // Limit amount of results returned by server. Non-positive value means no limit
    long queryMaxRecords = 10_000L;
    // Maximum batch size for batch read operations
    int batchReadSize = 1000;
    // Maximum batch size for batch write operations
    int batchWriteSize = 1000;
    // Define how @Id fields (primary keys) and Map keys are stored: false - always as String,
    // true - preserve original type if supported
    boolean keepOriginalKeyTypes = false;
    // Define how Maps and POJOs are written: true - as sorted maps (TreeMaps, default), false - as unsorted (HashMaps)
    // Writing unsorted maps (false) degrades performance of Map-related operations and does not allow comparing Maps,
    // strongly recommended not to use except during upgrade from older versions of Spring Data Aerospike (if required)
    boolean writeSortedMaps = true;
    // Name for the bin to store entity's class
    private String classKey = "@_class";
    // Fully qualified name of a class to be used for FieldNamingStrategy for entities
    private String fieldNamingStrategy;

    public AerospikeDataSettings(Environment environment) {
        if (environment != null) {
            setStringFromConfig(this::setNamespace, environment, CONFIG_PREFIX_DATA, "namespace");
            setBoolFromConfig(this::setScansEnabled, environment, CONFIG_PREFIX_DATA, "scansEnabled");
            setBoolFromConfig(this::setCreateIndexesOnStartup, environment, CONFIG_PREFIX_DATA,
                "createIndexesOnStartup");
            setIntFromConfig(this::setIndexCacheRefreshSeconds, environment, CONFIG_PREFIX_DATA,
                "indexCacheRefreshSeconds");
            setIntFromConfig(this::setServerVersionRefreshSeconds, environment, CONFIG_PREFIX_DATA,
                "serverVersionRefreshSeconds");
            setIntFromConfig(this::setQueryMaxRecords, environment, CONFIG_PREFIX_DATA, "queryMaxRecords");
            setIntFromConfig(this::setBatchWriteSize, environment, CONFIG_PREFIX_DATA, "batchWriteSize");
            setBoolFromConfig(this::setKeepOriginalKeyTypes, environment, CONFIG_PREFIX_DATA, "keepOriginalKeyTypes");
            setBoolFromConfig(this::setWriteSortedMaps, environment, CONFIG_PREFIX_DATA, "writeSortedMaps");
            setStringFromConfig(this::setClassKey, environment, CONFIG_PREFIX_DATA, "classKey");
            setStringFromConfig(this::setFieldNamingStrategy, environment, CONFIG_PREFIX_DATA, "fieldNamingStrategy");
        }
    }
}
