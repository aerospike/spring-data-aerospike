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
import org.springframework.boot.context.properties.ConfigurationProperties;

import static org.springframework.data.aerospike.config.AerospikeDataConfigurationSupport.CONFIG_PREFIX_DATA;

@Setter
@Getter
@ConfigurationProperties(prefix = CONFIG_PREFIX_DATA)
public class AerospikeDataSettings {

    // Namespace
    String namespace;
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
    // Maximum batch size for batch write operations
    int batchWriteSize = 100;
    // Define how @Id fields (primary keys) and Map keys are stored: false - always as String,
    // true - preserve original type if supported
    boolean keepOriginalKeyTypes = false;
    // Define how Maps and POJOs are written: true - as sorted maps (TreeMaps, default), false - as unsorted (HashMaps)
    // Writing unsorted maps (false) degrades performance of Map-related operations and does not allow comparing Maps,
    // strongly recommended not to use except during upgrade from older versions of Spring Data Aerospike (if required)
    boolean writeSortedMaps = true;
    // Name for the bin to store entity's class
    private String classKey = "@_class";
    // Fully qualified name of the FieldNamingStrategy for entities
    private Class<?> fieldNamingStrategy;
}
