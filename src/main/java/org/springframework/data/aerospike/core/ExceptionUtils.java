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

import com.aerospike.client.AerospikeException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;

/**
 * A utility class providing methods for handling and manipulating exceptions.
 */
public final class ExceptionUtils {

    private ExceptionUtils() {
        throw new UnsupportedOperationException("Utility class ExceptionUtils cannot be instantiated");
    }

    static RuntimeException translateError(AerospikeException e, AerospikeExceptionTranslator translator) {
        DataAccessException translated = translator.translateExceptionIfPossible(e);
        return translated == null ? e : translated;
    }

    static OptimisticLockingFailureException getOptimisticLockingFailureException(String errMsg,
                                                                                  AerospikeException e) {
        return new OptimisticLockingFailureException(errMsg, e);
    }

    static Throwable translateError(Throwable e, AerospikeExceptionTranslator translator) {
        if (e instanceof AerospikeException ae) {
            return translateError(ae, translator);
        }
        return e;
    }

    static Throwable translateCasThrowable(Throwable e, String operationName, TemplateContext templateContext) {
        if (e instanceof AerospikeException ae) {
            return translateCasError(ae, "Failed to %s record due to versions mismatch".formatted(operationName),
                templateContext.exceptionTranslator);
        }
        return e;
    }

    static RuntimeException translateCasError(AerospikeException e, String errMsg,
                                              AerospikeExceptionTranslator exceptionTranslator) {
        if (ValidationUtils.hasOptimisticLockingError(e.getResultCode())) {
            return getOptimisticLockingFailureException(errMsg, e);
        }
        return translateError(e, exceptionTranslator);
    }
}
