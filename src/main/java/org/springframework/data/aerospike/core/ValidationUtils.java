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

    static boolean areInvalidGroupedKeys(GroupedKeys groupedKeys) {
        Assert.notNull(groupedKeys, "Grouped keys must not be null!");
        return isEmpty(groupedKeys.getEntitiesKeys().keySet());
    }

    static boolean isEmpty(Iterable<?> iterable) {
        Assert.notNull(iterable, "Iterable must not be null!");
        return !iterable.iterator().hasNext();
    }

    static boolean hasOptimisticLockingError(int resultCode) {
        return List.of(ResultCode.GENERATION_ERROR, ResultCode.KEY_EXISTS_ERROR, ResultCode.KEY_NOT_FOUND_ERROR)
            .contains(resultCode);
    }

    static void verifyUnsortedWithOffset(Sort sort, long offset) {
        if ((sort == null || sort.isUnsorted())
            && offset > 0) {
            throw new IllegalArgumentException("Unsorted query must not have offset value. " +
                "For retrieving paged results use sorted query.");
        }
    }
}
