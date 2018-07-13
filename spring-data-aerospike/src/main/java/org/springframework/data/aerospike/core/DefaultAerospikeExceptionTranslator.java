/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.springframework.dao.*;
import org.springframework.data.keyvalue.core.UncategorizedKeyValueException;

/**
 * @author Peter Milne
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
			switch (resultCode) {
			/*
			 * Future enhancements will be more elaborate 
			 */
				case ResultCode.KEY_EXISTS_ERROR:
					return new DuplicateKeyException(msg, cause);
				case ResultCode.KEY_NOT_FOUND_ERROR:
					return new DataRetrievalFailureException(msg, cause);
				case ResultCode.INDEX_NOTFOUND:
				case ResultCode.INDEX_ALREADY_EXISTS:
					return new InvalidDataAccessResourceUsageException(msg, cause);
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
		return new UncategorizedKeyValueException("Unexpected Aerospike Exception", cause);
	}
}
