/*
 * Copyright 2015 the original author or authors.
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
import com.aerospike.client.IAerospikeClient;

/**
 * Callback to interact with the {@link IAerospikeClient}.
 * 
 * @author Oliver Gierke
 * @author Peter Milne
 */
public interface AerospikeClientCallback<T> {

	/**
	 * Interact with the native {@link IAerospikeClient} and produce a records from it.
	 *
	 * @param client will never be {@literal null}.
	 * @return Record iterator
	 * @throws AerospikeException in case of encountering an error while interacting
	 * 		with IAerospikeClient an AerospikeException will be thrown.
	 */
	T recordIterator(IAerospikeClient client) throws AerospikeException;

	/**
	 * Interact with the native {@link IAerospikeClient} and produce a result from it.
	 *
	 * @param client will never be {@literal null}.
	 * @return Result iterator
	 * @throws AerospikeException in case of encountering an error while interacting
	 * 		with IAerospikeClient an AerospikeException will be thrown.
	 */
	T resultIterator(IAerospikeClient client) throws AerospikeException;
}
