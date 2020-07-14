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

import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.query.model.IndexKey;
import org.springframework.data.aerospike.query.model.IndexesInfo;

import java.util.Arrays;
import java.util.Collections;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toMap;

/**
 * Internal index related operations used by ReactorIndexRefresher and IndexRefresher.
 *
 * @author Sergii Karpenko
 */
public class InternalIndexOperations {

	private static final String SINDEX = "sindex";

	private final IndexInfoParser indexInfoParser;

	public InternalIndexOperations(IndexInfoParser indexInfoParser) {
		this.indexInfoParser = indexInfoParser;
	}

	public IndexesInfo parseIndexesInfo(String infoResponse) {
		if (infoResponse.isEmpty()) {
			return IndexesInfo.empty();
		}
		return IndexesInfo.of(Arrays.stream(infoResponse.split(";"))
				.map(indexInfoParser::parse)
				.collect(collectingAndThen(
						toMap(InternalIndexOperations::getIndexKey, index -> index),
						Collections::unmodifiableMap)));
	}

	public String buildGetIndexesCommand() {
		return SINDEX;
	}

	private static IndexKey getIndexKey(Index index) {
		return new IndexKey(index.getNamespace(), index.getSet(), index.getBin(), index.getType());
	}

}