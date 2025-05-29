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

import com.aerospike.client.ResultCode;
import org.springframework.data.aerospike.core.model.GroupedKeys;
import org.springframework.data.domain.Sort;
import org.springframework.util.Assert;

import java.util.List;

/**
 * A utility class providing methods to perform validation checks.
 */
public final class ValidationUtils {

    private ValidationUtils() {
        throw new UnsupportedOperationException("Utility class ValidationUtils cannot be instantiated");
    }

    /**
     * Checks if the provided {@code GroupedKeys} object contains invalid or empty grouped keys. A {@code GroupedKeys}
     * object is considered invalid if the key set of its entities is empty.
     *
     * @param groupedKeys The {@link GroupedKeys} object to check. Must not be null
     * @return {@code true} if the grouped keys are invalid (i.e., the key set of entities is empty), {@code false}
     * otherwise
     * @throws IllegalArgumentException If {@code groupedKeys} is null
     */
    static boolean areInvalidGroupedKeys(GroupedKeys groupedKeys) {
        Assert.notNull(groupedKeys, "Grouped keys must not be null!");
        return isEmpty(groupedKeys.getEntitiesKeys().keySet());
    }

    /**
     * Checks if an {@code Iterable} is empty. An {@code Iterable} is considered empty if its iterator has no more
     * elements.
     *
     * @param iterable The {@link Iterable} to check. Must not be null
     * @return {@code true} if the iterable is empty, {@code false} otherwise
     * @throws IllegalArgumentException If {@code iterable} is null
     */
    static boolean isEmpty(Iterable<?> iterable) {
        Assert.notNull(iterable, "Iterable must not be null!");
        return !iterable.iterator().hasNext();
    }

    /**
     * Determines if a given result code indicates an optimistic locking error. Optimistic locking errors are identified
     * by specific {@code ResultCode} values: {@link ResultCode#GENERATION_ERROR}, {@link ResultCode#KEY_EXISTS_ERROR},
     * and {@link ResultCode#KEY_NOT_FOUND_ERROR}.
     *
     * @param resultCode The integer result code to evaluate
     * @return {@code true} if the {@code resultCode} corresponds to an optimistic locking error, {@code false}
     * otherwise
     */
    static boolean hasOptimisticLockingError(int resultCode) {
        return List.of(ResultCode.GENERATION_ERROR, ResultCode.KEY_EXISTS_ERROR, ResultCode.KEY_NOT_FOUND_ERROR)
            .contains(resultCode);
    }

    /**
     * Verifies that an unsorted query does not have an offset value. If the {@code Sort} object is null or indicates an
     * unsorted state, and the offset is greater than 0, an {@code IllegalArgumentException} is thrown. Paged results
     * for unsorted queries are not supported.
     *
     * @param sort   The {@link Sort} object indicating the sorting criteria. Can be null
     * @param offset The offset value for the query. Must be 0 if the query is unsorted
     * @throws IllegalArgumentException If the query is unsorted and has an offset greater than 0
     */
    static void verifyUnsortedWithOffset(Sort sort, long offset) {
        if ((sort == null || sort.isUnsorted())
            && offset > 0) {
            throw new IllegalArgumentException("Unsorted query must not have offset value. " +
                "For retrieving paged results use sorted query.");
        }
    }
}
