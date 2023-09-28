/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.exceptions;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;

import java.util.Map;
import java.util.stream.Collectors;

public class BatchWriteException extends AerospikeException {

    public BatchWriteException(Map<String, String> keysAndReasons) {
        super(ResultCode.BATCH_FAILED, errMsg(keysAndReasons));
    }

    public BatchWriteException(Map<String, String> keysAndReasons, Throwable cause) {
        super(ResultCode.BATCH_FAILED, errMsg(keysAndReasons), cause);
    }

    private static String errMsg(Map<String, String> keysAndReasons) {
        return "Errors during batch write with the following keys: " + keysAndReasonsToString(keysAndReasons);
    }

    private static String keysAndReasonsToString(Map<String, String> keysAndReasons) {
        if (keysAndReasons == null || keysAndReasons.isEmpty()) return "";

        return keysAndReasons.entrySet().stream()
            .map(entry -> entry.getKey() + ", reason: " + entry.getValue())
            .collect(Collectors.joining(", "));
    }
}
