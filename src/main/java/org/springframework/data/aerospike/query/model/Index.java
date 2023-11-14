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
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

/**
 * This class represents a Secondary Index created in the cluster.
 *
 * @author Peter Milne
 * @author Anastasiia Smirnova
 */
@Builder
@RequiredArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@Getter
public class Index {

    private final String name;
    private final String namespace;
    private final String set;
    private final String bin;
    private final IndexType indexType;
    private final IndexCollectionType indexCollectionType;
    private final CTX[] ctx;
    @Setter
    private int binValuesRatio;

    public Index(String name, String namespace, String set, String bin, IndexType indexType,
                 IndexCollectionType indexCollectionType) {
        this.name = name;
        this.namespace = namespace;
        this.set = set;
        this.bin = bin;
        this.indexType = indexType;
        this.indexCollectionType = indexCollectionType;
        this.ctx = null;
    }
}
