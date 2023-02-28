/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.aerospike.core;

import com.aerospike.client.Bin;
import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import org.springframework.lang.Nullable;

import java.util.Map;
import java.util.function.Function;

public class OperationUtils {

    static <T> Operation[] operations(Map<String, T> values,
                                      Operation.Type operationType,
                                      Operation... additionalOperations) {
        Operation[] operations = new Operation[values.size() + additionalOperations.length];
        int x = 0;
        for (Map.Entry<String, T> entry : values.entrySet()) {
            operations[x] = new Operation(operationType, entry.getKey(), Value.get(entry.getValue()));
            x++;
        }
        for (Operation additionalOp : additionalOperations) {
            operations[x] = additionalOp;
            x++;
        }
        return operations;
    }

    static Operation[] operations(Bin[] bins, Function<Bin, Operation> binToOperation) {
        return operations(bins, binToOperation, null, null);
    }

    static Operation[] operations(Bin[] bins, Function<Bin, Operation> binToOperation,
                                  Operation[] precedingOperations) {
        return operations(bins, binToOperation, precedingOperations, null);
    }

    static Operation[] operations(Bin[] bins,
                                  Function<Bin, Operation> binToOperation,
                                  @Nullable Operation[] precedingOperations,
                                  @Nullable Operation[] additionalOperations) {
        int precedingOpsLength = precedingOperations == null ? 0 : precedingOperations.length;
        int additionalOpsLength = additionalOperations == null ? 0 : additionalOperations.length;
        Operation[] operations = new Operation[precedingOpsLength + bins.length + additionalOpsLength];
        int i = 0;
        if (precedingOpsLength > 0) {
            for (Operation precedingOp : precedingOperations) {
                operations[i] = precedingOp;
                i++;
            }
        }
        for (Bin bin : bins) {
            operations[i] = binToOperation.apply(bin);
            i++;
        }
        if (additionalOpsLength > 0) {
            for (Operation additionalOp : additionalOperations) {
                operations[i] = additionalOp;
                i++;
            }
        }
        return operations;
    }
}
