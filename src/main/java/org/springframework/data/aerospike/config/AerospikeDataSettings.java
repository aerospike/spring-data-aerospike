/*
 * Copyright 2020 the original author or authors.
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

import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class AerospikeDataSettings {

    @Builder.Default
    // Enable scan operation
    boolean scansEnabled = false;
    @Builder.Default
    // Send user defined key in addition to hash digest on both reads and writes
    boolean sendKey = true;
    @Builder.Default
    // Create secondary indexes specified using `@Indexed` annotation on startup
    boolean createIndexesOnStartup = true;
    @Builder.Default
    // Automatically refresh indexes cache every <N> seconds
    int indexCacheRefreshFrequencySeconds = 3600;
    @Builder.Default
    // Limit amount of results returned by server. Non-positive value means no limit
    long queryMaxRecords = 10_000L;
    // Define how @Id fields (primary keys) and Map keys are stored: false - always as String,
    // true - preserve original type if supported
    @Builder.Default
    boolean keepOriginalKeyTypes = false;

    /*
     * (non-Javadoc)
     * Javadoc is not aware of the code modifications made by Lombok.
     * You can fix it with either delombok or by adding a static inner class inside the class that uses the @Builder
     * annotation,
     * it will satisfy javadoc and won't interfere with the @Builder annotation's normal behaviour.
     */
    public static class AerospikeDataSettingsBuilder {

    }
}
