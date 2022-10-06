/*
 * Copyright 2012-2020 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.springframework.data.aerospike.query.model;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.Value;

/**
 * This class represents a Secondary Index
 * created in the cluster.
 *
 * @author Peter Milne
 * @author Anastasiia Smirnova
 */
@Value
@Builder
@RequiredArgsConstructor
public class Index {

	String name;
	String namespace;
	String set;
	String bin;
	IndexType indexType;
	IndexCollectionType indexCollectionType;
	CTX[] ctx;

	public Index(String name, String namespace, String set, String bin, IndexType indexType, IndexCollectionType indexCollectionType) {
		this.name = name;
		this.namespace = namespace;
		this.set = set;
		this.bin = bin;
		this.indexType = indexType;
		this.indexCollectionType = indexCollectionType;
		this.ctx = null;
	}

	public String getName() {
		return this.name;
	}

	public String getBin() {
		return this.bin;
	}

	public String getSet() {
		return this.set;
	}

	public String getNamespace() {
		return this.namespace;
	}

	public IndexType getType() {
		return this.indexType;
	}

	public IndexCollectionType getCollectionType() {
		return this.indexCollectionType;
	}

	public CTX[] getCTX() {
		return this.ctx;
	}
}
