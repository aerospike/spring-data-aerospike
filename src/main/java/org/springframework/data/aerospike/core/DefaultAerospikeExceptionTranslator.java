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
import org.springframework.data.aerospike.IndexAlreadyExistsException;
import org.springframework.data.aerospike.IndexNotFoundException;

/**
 * @author Peter Milne
 * @author Anastasiia Smirnova
 * This class translates the AerospikeException and result code
 * to a DataAccessException.
 */
public class DefaultAerospikeExceptionTranslator implements AerospikeExceptionTranslator {

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.dao.support.PersistenceExceptionTranslator#translateExceptionIfPossible(java.lang.RuntimeException)
	 */
	@Override
	public DataAccessException translateExceptionIfPossible(RuntimeException cause) {

		if (cause instanceof AerospikeException){
			int resultCode = ((AerospikeException)cause).getResultCode();
			String msg = cause.getMessage();
			if (cause instanceof AerospikeException.Connection) {
				if (resultCode == ResultCode.SERVER_NOT_AVAILABLE) {
					// we should throw query timeout exception only when opening new connection fails with SocketTimeoutException.
					// see com.aerospike.client.cluster.Connection for more details
					return new QueryTimeoutException(msg, cause);
				}
			}
			switch (resultCode) {
			/*
			 * Future enhancements will be more elaborate 
			 */
				case ResultCode.PARAMETER_ERROR:
					return new InvalidDataAccessApiUsageException(msg, cause);
				case ResultCode.KEY_EXISTS_ERROR:
					return new DuplicateKeyException(msg, cause);
				case ResultCode.KEY_NOT_FOUND_ERROR:
					return new DataRetrievalFailureException(msg, cause);
				case ResultCode.INDEX_NOTFOUND:
					return new IndexNotFoundException(msg, cause);
				case ResultCode.INDEX_ALREADY_EXISTS:
					return new IndexAlreadyExistsException(msg, cause);
				case ResultCode.TIMEOUT:
				case ResultCode.QUERY_TIMEOUT:
					return new QueryTimeoutException(msg, cause);
				case ResultCode.DEVICE_OVERLOAD:
				case ResultCode.NO_MORE_CONNECTIONS:
				case ResultCode.KEY_BUSY:
					return new TransientDataAccessResourceException(msg, cause);

				default:
					return new RecoverableDataAccessException(msg, cause);

			}
		}
		//we should not convert exceptions that spring-data-aeropike does not recognise
		return null;
	}
}
