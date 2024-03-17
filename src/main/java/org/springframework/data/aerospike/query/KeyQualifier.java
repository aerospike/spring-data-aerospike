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
package org.springframework.data.aerospike.query;

import com.aerospike.client.Key;
import com.aerospike.client.Value;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.AerospikeRepository;
import org.springframework.data.aerospike.repository.support.SimpleAerospikeRepository;

import java.io.Serial;

import static org.springframework.data.aerospike.query.qualifier.QualifierKey.DIGEST_KEY;

/**
 * Qualifier used to query by primary key
 *
 * @author peter
 * @deprecated Since 4.6.0. Use {@link SimpleAerospikeRepository#findById(Object)} or
 * {@link AerospikeRepository#findUsingQuery(org.springframework.data.aerospike.repository.query.Query)} with
 * {@link Qualifier#idEquals(String)}
 */
@Deprecated(since = "4.6.0", forRemoval = true)
public class KeyQualifier extends Qualifier {

    @Serial
    private static final long serialVersionUID = 2430949321378171078L;

    boolean hasDigest = false;

    public KeyQualifier(Value value) {
        super(Qualifier.builder()
            .setField(QueryEngine.Meta.KEY.toString())
            .setFilterOperation(FilterOperation.EQ)
            .setValue(value)
        );
    }

    public KeyQualifier(byte[] digest) {
        super(Qualifier.builder()
            .setField(QueryEngine.Meta.KEY.toString())
            .setFilterOperation(FilterOperation.EQ)
            .setValue(null)
        );
        this.internalMap.put(DIGEST_KEY, digest);
        this.hasDigest = true;
    }

    @Override
    protected String luaFieldString(String field) {
        return "digest";
    }

    public byte[] getDigest() {
        return (byte[]) this.internalMap.get(DIGEST_KEY);
    }

    public Key makeKey(String namespace, String set) {
        if (hasDigest) {
            byte[] digest = getDigest();
            return new Key(namespace, digest, set, null);
        }
        return new Key(namespace, set, getValue());
    }
}
