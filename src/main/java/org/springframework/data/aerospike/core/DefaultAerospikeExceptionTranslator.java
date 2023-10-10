/*
 * Copyright 2019 the original author or authors.
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
import com.aerospike.client.ResultCode;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.data.aerospike.exceptions.IndexAlreadyExistsException;
import org.springframework.data.aerospike.exceptions.IndexNotFoundException;

/**
 * This class translates the AerospikeException and result code to a DataAccessException.
 *
 * @author Peter Milne
 * @author Anastasiia Smirnova
 */
public class DefaultAerospikeExceptionTranslator implements AerospikeExceptionTranslator {

    @Override
    public DataAccessException translateExceptionIfPossible(RuntimeException cause) {
        if (cause instanceof AerospikeException) {
            int resultCode = ((AerospikeException) cause).getResultCode();
            String msg = cause.getMessage();
            if (cause instanceof AerospikeException.Connection) {
                if (resultCode == ResultCode.SERVER_NOT_AVAILABLE) {
                    // we should throw query timeout exception only when opening new connection fails with
                    // SocketTimeoutException.
                    // see com.aerospike.client.cluster.Connection for more details.
                    return new QueryTimeoutException(msg, cause);
                }
            }
            return switch (resultCode) {
                case ResultCode.PARAMETER_ERROR -> new InvalidDataAccessApiUsageException(msg, cause);
                case ResultCode.KEY_EXISTS_ERROR -> new DuplicateKeyException(msg, cause);
                case ResultCode.KEY_NOT_FOUND_ERROR -> new DataRetrievalFailureException(msg, cause);
                case ResultCode.INDEX_NOTFOUND -> new IndexNotFoundException(msg, cause);
                case ResultCode.INDEX_ALREADY_EXISTS -> new IndexAlreadyExistsException(msg, cause);
                case ResultCode.TIMEOUT, ResultCode.QUERY_TIMEOUT -> new QueryTimeoutException(msg, cause);
                case ResultCode.DEVICE_OVERLOAD, ResultCode.NO_MORE_CONNECTIONS, ResultCode.KEY_BUSY ->
                    new TransientDataAccessResourceException(msg, cause);
                default -> new RecoverableDataAccessException(msg, cause);
            };
        }
        // we should not convert exceptions that spring-data-aerospike does not recognise.
        return null;
    }
}
