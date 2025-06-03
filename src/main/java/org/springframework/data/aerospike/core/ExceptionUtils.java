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

    /**
     * Translates an {@link AerospikeException} into a Spring {@link DataAccessException} if possible. If the exception
     * cannot be translated, the original {@link AerospikeException} is returned.
     *
     * @param e          The {@link AerospikeException} to translate
     * @param translator The {@link AerospikeExceptionTranslator} to use for translation
     * @return A {@link DataAccessException} if translation is successful, otherwise the original
     * {@link AerospikeException}
     */
    static RuntimeException translateError(AerospikeException e, AerospikeExceptionTranslator translator) {
        DataAccessException translated = translator.translateExceptionIfPossible(e);
        return translated == null ? e : translated;
    }

    /**
     * Translates a generic {@link Throwable} into a more specific exception. If the throwable is an
     * {@link AerospikeException}, it delegates to
     * {@link #translateError(AerospikeException, AerospikeExceptionTranslator)}. Otherwise, the original throwable is
     * returned.
     *
     * @param e          The {@link Throwable} to translate
     * @param translator The {@link AerospikeExceptionTranslator} to use for translation
     * @return A translated {@link RuntimeException} or the original {@link Throwable}
     */
    static Throwable translateError(Throwable e, AerospikeExceptionTranslator translator) {
        if (e instanceof AerospikeException ae) {
            return translateError(ae, translator);
        }
        return e;
    }

    /**
     * Translates a {@link Throwable} that might be related to a CAS operation failure. If the throwable is an
     * {@link AerospikeException}, it attempts to translate it into an {@link OptimisticLockingFailureException} if the
     * result code indicates an optimistic locking error. Otherwise, it delegates to general error translation.
     *
     * @param e               The {@link Throwable} to translate
     * @param operationName   The name of the operation that was attempted (e.g., "save", "update")
     * @param templateContext The context containing the exception translator
     * @return A translated {@link RuntimeException} (potentially {@link OptimisticLockingFailureException}) or the
     * original {@link Throwable}
     */
    static Throwable translateCasThrowable(Throwable e, String operationName, TemplateContext templateContext) {
        if (e instanceof AerospikeException ae) {
            return translateCasException(ae, "Failed to %s record due to versions mismatch".formatted(operationName),
                templateContext.exceptionTranslator);
        }
        return e;
    }

    /**
     * Translates an {@link AerospikeException} that might be an optimistic locking error. If the exception's result
     * code indicates an optimistic locking error, it is translated into an {@link OptimisticLockingFailureException}.
     * Otherwise, it falls back to general error translation.
     *
     * @param e                   The {@link AerospikeException} to translate
     * @param errMsg              The error message to use if an {@link OptimisticLockingFailureException} is created
     * @param exceptionTranslator The {@link AerospikeExceptionTranslator} to use for general error translation
     * @return A {@link RuntimeException}, which will be an {@link OptimisticLockingFailureException} if a CAS error is
     * detected
     */
    static RuntimeException translateCasException(AerospikeException e, String errMsg,
                                                  AerospikeExceptionTranslator exceptionTranslator) {
        if (ValidationUtils.hasOptimisticLockingError(e.getResultCode())) {
            return new OptimisticLockingFailureException(errMsg, e);
        }
        return translateError(e, exceptionTranslator);
    }
}
