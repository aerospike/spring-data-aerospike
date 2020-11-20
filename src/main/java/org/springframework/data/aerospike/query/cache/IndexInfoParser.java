/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	  https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.query.cache;

import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import org.springframework.data.aerospike.query.model.Index;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author Peter Milne
 * @author Anastasiia Smirnova
 */
public class IndexInfoParser {

	private static final String INDEX_NAME = "indexname";
	private static final String BIN = "bin";
	private static final String BINS = "bins";
	private static final String SET = "set";
	private static final String NS = "ns";
	private static final String NAMESPACE = "namespace";
	private static final String TYPE = "type";
	private static final String STRING_TYPE = "STRING";
	private static final String GEO_JSON_TYPE = "GEOJSON";
	private static final String NUMERIC_TYPE = "NUMERIC";
	private static final String NONE = "NONE";
	private static final String LIST = "LIST";
	private static final String MAPKEYS = "MAPKEYS";
	private static final String MAPVALUES = "MAPVALUES";

	public Index parse(String infoString) {
		Map<String, String> values = getIndexInfo(infoString);
		String name = getRequiredName(values);
		String namespace = getRequiredNamespace(values);
		String set = getNullableSet(values);
		String bin = getRequiredBin(values);
		IndexType indexType = getIndexTypeInternal(values);
		IndexCollectionType collectionType = getIndexCollectionTypeInternal(values);
		return Index.builder()
				.name(name)
				.namespace(namespace)
				.set(set)
				.bin(bin)
				.indexType(indexType)
				.indexCollectionType(collectionType)
				.build();
	}

	/**
	 * Populates the Index object from an "info" message from Aerospike
	 *
	 * @param info Info string from node
	 */
	private Map<String, String> getIndexInfo(String info) {
		//ns=phobos_sindex:set=longevity:indexname=str_100_idx:num_bins=1:bins=str_100_bin:type=TEXT:sync_state=synced:state=RW;
		//ns=test:set=Customers:indexname=mail_index_userss:bin=email:type=STRING:indextype=LIST:path=email:sync_state=synced:state=RW
		if (info.isEmpty()) {
			return Collections.emptyMap();
		}
		return Arrays.stream(info.split(":"))
				.map(part -> {
					String[] kvParts = part.split("=");
					if (kvParts.length == 0) {
						throw new IllegalStateException("Failed to parse info string part: " + part);
					}
					if (kvParts.length == 1) {
						return null;
					}
					return new AbstractMap.SimpleEntry<>(kvParts[0], kvParts[1]);
				})
				.filter(Objects::nonNull)
				.collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
	}

	private String getRequiredName(Map<String, String> values) {
		String name = values.get(INDEX_NAME);
		if (name != null)
			return name;
		throw new IllegalStateException("Name not present in info: " + values);
	}

	private IndexType getIndexTypeInternal(Map<String, String> values) {
		String indexTypeString = values.get(TYPE);
		if (indexTypeString == null) {
			throw new IllegalStateException("Index type not present in info: " + values);
		}
		if (indexTypeString.equalsIgnoreCase(STRING_TYPE))
			return IndexType.STRING;
		else if (indexTypeString.equalsIgnoreCase(GEO_JSON_TYPE))
			return IndexType.GEO2DSPHERE;
		else if (indexTypeString.equalsIgnoreCase(NUMERIC_TYPE))
			return IndexType.NUMERIC;
		return null;
//        TODO: should we throw exception in case of unknown index type? (Anastasiia Smirnova)
//        throw new IllegalStateException("Unknown index type: " + indexTypeString + " in info: " + values);
	}

	private IndexCollectionType getIndexCollectionTypeInternal(Map<String, String> values) {
		String indexTypeString = values.get("indextype");
		if (indexTypeString == null) {
			throw new IllegalStateException("Index type not present in info: " + values);
		}
		if (indexTypeString.equalsIgnoreCase(NONE))
			return IndexCollectionType.DEFAULT;
		else if (indexTypeString.equalsIgnoreCase(LIST))
			return IndexCollectionType.LIST;
		else if (indexTypeString.equalsIgnoreCase(MAPKEYS))
			return IndexCollectionType.MAPKEYS;
		else if (indexTypeString.equalsIgnoreCase(MAPVALUES))
			return IndexCollectionType.MAPVALUES;
		return null;
	}

	private String getRequiredBin(Map<String, String> values) {
		String bin = values.get(BIN);
		if (bin != null)
			return bin;
		String bins = values.get(BINS);
		if (bins != null)
			return bins;
		throw new IllegalStateException("Bin not present in info: " + values);
	}

	private String getNullableSet(Map<String, String> values) {
		String set = values.get(SET);
		if (set != null && set.equalsIgnoreCase("null")) {
			return null;
		}
		return set;
	}

	private String getRequiredNamespace(Map<String, String> values) {
		String ns = values.get(NS);
		if (ns != null)
			return ns;
		String namespace = values.get(NAMESPACE);
		if (namespace != null)
			return namespace;
		throw new IllegalStateException("Namespace not present in info: " + values);
	}
}
