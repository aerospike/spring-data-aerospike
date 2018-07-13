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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import org.springframework.data.aerospike.convert.AerospikeConverter;
import org.springframework.data.aerospike.convert.AerospikeReadData;
import org.springframework.data.aerospike.convert.AerospikeWriteData;
import org.springframework.data.aerospike.utility.Utils;
import org.springframework.data.keyvalue.core.AbstractKeyValueAdapter;
import org.springframework.data.keyvalue.core.KeyValueAdapter;
import org.springframework.data.keyvalue.core.KeyValueTemplate;
import org.springframework.data.keyvalue.core.query.KeyValueQuery;
import org.springframework.data.util.CloseableIterator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

/**
 * An Aerospike-specific {@link KeyValueAdapter} to implement core sore interactions to be used by the
 * {@link KeyValueTemplate}.
 * 
 * @author Oliver Gierke
 */
public class AerospikeKeyValueAdapter extends AbstractKeyValueAdapter {

	private final AerospikeConverter converter;
	private final AerospikeClient client;

	private String namespace;
	private final WritePolicy insertPolicy;
	private final WritePolicy updatePolicy;

	/**
	 * Creates a new {@link AerospikeKeyValueAdapter} using the given {@link AerospikeClient} and
	 * {@link AerospikeConverter}.
	 * 
	 * @param client must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 */
	public AerospikeKeyValueAdapter(AerospikeClient client, AerospikeConverter converter, String namespace) {

		this.client = client;
		this.converter = converter;
		this.namespace = namespace;
		this.insertPolicy = new WritePolicy(this.client.writePolicyDefault);
		this.updatePolicy = new WritePolicy(this.client.writePolicyDefault);
		this.insertPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
		this.updatePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
	}

	
	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueAdapter#put(java.io.Serializable, java.lang.Object, java.io.Serializable)
	 */
	@Override
	public Object put(Serializable id, Object item, Serializable keyspace) {
		AerospikeWriteData data = AerospikeWriteData.forWrite();

		converter.write(item, data);

		client.put(null, data.getKey(), data.getBinsAsArray());

		return item;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueAdapter#contains(java.io.Serializable, java.io.Serializable)
	 */
	@Override
	public boolean contains(Serializable id, Serializable keyspace) {

		return client.exists(null, makeKey(keyspace.toString(), id.toString()));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueAdapter#get(java.io.Serializable, java.io.Serializable)
	 */
	@Override
	public Object get(Serializable id, Serializable keyspace) {

		Key key = makeKey(keyspace.toString(), id.toString());
		Record record = client.get(null, key);
		if(record == null){
			return null;
		}
		AerospikeReadData data = AerospikeReadData.forRead(key, record);
		return converter.read(Object.class,  data);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueAdapter#delete(java.io.Serializable, java.io.Serializable)
	 */
	@Override
	public Object delete(Serializable id, Serializable keyspace) {

		Key key = new Key(namespace, keyspace.toString(), id.toString());

		Object object = get(id, keyspace);

		if (object != null) {
			WritePolicy wp = new WritePolicy();
			wp.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
			client.delete(wp, key);
		}

		return object;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueAdapter#getAllOf(java.io.Serializable)
	 */
	@Override
	public Collection<?> getAllOf(Serializable keyspace) {
		
		Statement statement = new Statement();
		statement.setNamespace(namespace);
		statement.setSetName(keyspace.toString());

		
		List<Object> result = new ArrayList<Object>();
		RecordSet recordSet = client.query(null, statement);
		
		while (recordSet.next()) {

			//AerospikeData data = AerospikeData.forRead(recordSet.getKey(), recordSet.getRecord());
			//result.add(converter.read(Object.class, data));
		}

//		final List<Object> result = new ArrayList<Object>();
//		String set = keyspace.toString();
//		client.scanAll(null, namespace, set, new ScanCallback() {
//			
//			@Override
//			public void scanCallback(Key key, Record record) throws AerospikeException {
//				AerospikeData data = AerospikeData.forRead(key, record);
//				Object converted = converter.read(Object.class, data);
//				result.add(converted);
//			}
//		});
		return result;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueAdapter#deleteAllOf(java.io.Serializable)
	 */
	@Override
	public void deleteAllOf(Serializable keyspace) {
		//"set-config:context=namespace;id=namespace_name;set=set_name;set-delete=true;"
		Utils.infoAll(client, "set-config:context=namespace;id=" + this.namespace + ";set=" + keyspace.toString() + ";set-delete=true;");
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.KeyValueAdapter#clear()
	 */
	@Override
	public void clear() {}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	@Override
	public void destroy() throws Exception {}
	
	private Key makeKey(String set, Object keyValue){
		return new Key(this.namespace, set, Value.get(keyValue));
	}
	
	@Override
	public Collection<?> find(KeyValueQuery<?> query, Serializable keyspace) {
		// TODO Auto-generated method stub
		return super.find(query, keyspace);
	}

	@Override
	public CloseableIterator<Entry<Serializable, Object>> entries(
			Serializable keyspace) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long count(Serializable keyspace) {
		// TODO Auto-generated method stub
		return 0;
	}
	

}
