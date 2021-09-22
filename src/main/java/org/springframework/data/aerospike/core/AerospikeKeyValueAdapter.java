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

import com.aerospike.client.*;
import com.aerospike.client.Record;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import org.springframework.data.aerospike.convert.AerospikeConverter;
import org.springframework.data.aerospike.convert.AerospikeReadData;
import org.springframework.data.aerospike.convert.AerospikeWriteData;
import org.springframework.data.aerospike.utility.Utils;
import org.springframework.data.keyvalue.core.AbstractKeyValueAdapter;
import org.springframework.data.keyvalue.core.KeyValueAdapter;
import org.springframework.data.keyvalue.core.KeyValueTemplate;
import org.springframework.data.keyvalue.core.query.KeyValueQuery;
import org.springframework.data.util.CloseableIterator;

import java.util.Collection;
import java.util.Map.Entry;

/**
 * An Aerospike-specific {@link KeyValueAdapter} to implement core sore interactions to be used by the
 * {@link KeyValueTemplate}.
 *
 * @author Oliver Gierke
 */
public class AerospikeKeyValueAdapter extends AbstractKeyValueAdapter {

	private final AerospikeConverter converter;
	private final IAerospikeClient client;
	private final String namespace;
	private final WritePolicy insertPolicy;
	private final WritePolicy updatePolicy;

	/**
	 * Creates a new {@link AerospikeKeyValueAdapter} using the given {@link IAerospikeClient} and
	 * {@link AerospikeConverter}.
	 *
	 * @param client    must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 */
	public AerospikeKeyValueAdapter(IAerospikeClient client, AerospikeConverter converter, String namespace) {
		this.client = client;
		this.converter = converter;
		this.namespace = namespace;
		this.insertPolicy = WritePolicyBuilder.builder(client.getWritePolicyDefault())
				.recordExistsAction(RecordExistsAction.CREATE_ONLY)
				.build();
		this.updatePolicy = WritePolicyBuilder.builder(client.getWritePolicyDefault())
				.recordExistsAction(RecordExistsAction.UPDATE_ONLY)
				.build();
	}

	@Override
	public Object put(Object id, Object item, String keyspace) {
		AerospikeWriteData data = AerospikeWriteData.forWrite(namespace);
		converter.write(item, data);
		client.put(null, data.getKey(), data.getBinsAsArray());
		return item;
	}

	@Override
	public boolean contains(Object id, String keyspace) {
		return client.exists(null, makeKey(keyspace, id.toString()));
	}

	@Override
	public Object get(Object id, String keyspace) {
		Key key = makeKey(keyspace, id.toString());
		Record record = client.get(null, key);
		if(record == null){
			return null;
		}
		AerospikeReadData data = AerospikeReadData.forRead(key, record);
		return converter.read(Object.class,  data);
	}

	@Override
	public Object delete(Object id, String keyspace) {
		Key key = new Key(namespace, keyspace, id.toString());
		Object object = get(id, keyspace);
		if (object != null) {
			WritePolicy writePolicy = WritePolicyBuilder.builder(client.getWritePolicyDefault())
					.recordExistsAction(RecordExistsAction.UPDATE_ONLY)
					.build();
			client.delete(writePolicy, key);
		}
		return object;
	}

	@Override
	public Collection<?> getAllOf(String keyspace) {
		return null;
	}

	@Override
	public void deleteAllOf(String keyspace) {
		//"set-config:context=namespace;id=namespace_name;set=set_name;set-delete=true;"
		Utils.infoAll(client, "set-config:context=namespace;id=" + this.namespace + ";set=" + keyspace + ";set-delete=true;");
	}

	@Override
	public void clear() {}

	@Override
	public void destroy() throws Exception {}
	
	@Override
	public Collection<?> find(KeyValueQuery<?> query, String keyspace) {
		// TODO Auto-generated method stub
		return super.find(query, keyspace);
	}

	@Override
	public CloseableIterator<Entry<Object, Object>> entries(
			String keyspace) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long count(String keyspace) {
		// TODO Auto-generated method stub
		return 0;
	}

	private Key makeKey(String set, Object keyValue){
		return new Key(this.namespace, set, Value.get(keyValue));
	}
}
