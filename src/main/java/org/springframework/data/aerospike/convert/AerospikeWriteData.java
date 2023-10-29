/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.aerospike.convert;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;

/**
 * Value object to carry data to be written in object conversion.
 *
 * @author Oliver Gierke
 * @author Anastasiia Smirnova
 */
@Setter
public class AerospikeWriteData {

    @Getter
    private Key key;
    @Getter
    private final String namespace;
    @Getter
    private String setName;
    private Collection<Bin> bins;
    @Getter
    private int expiration;
    private Integer version;
    @Getter
    private Collection<String> requestedBins;

    public AerospikeWriteData(Key key, Collection<Bin> bins, int expiration, Integer version) {
        this(key, key.namespace, bins, expiration, version, Collections.emptyList());
    }

    public AerospikeWriteData(Key key, String namespace, Collection<Bin> bins, int expiration, Integer version,
                              Collection<String> requestedBins) {
        this.key = key;
        this.namespace = namespace;
        this.bins = bins;
        this.expiration = expiration;
        this.version = version;
        this.requestedBins = requestedBins;
    }

    /**
     * Initialize an {@link AerospikeWriteData} object with a namespace required for subsequent writing
     */
    public static AerospikeWriteData forWrite(String namespace) {
        return new AerospikeWriteData(null, namespace, new ArrayList<>(), 0, null, Collections.emptyList());
    }

    public Collection<Bin> getBins() {
        return Collections.unmodifiableCollection(bins);
    }

    public Bin[] getBinsAsArray() {
        return bins.toArray(new Bin[0]);
    }

    public boolean hasRequestedBins() {
        return requestedBins != null && !requestedBins.isEmpty();
    }

    public Optional<Integer> getVersion() {
        return Optional.ofNullable(version);
    }

    public void addBin(String key, Object value) {
        if (value instanceof Map<?, ?> map) {
            if (value instanceof SortedMap<?, ?> sortedMap) {
                add(new Bin(key, sortedMap));
            } else {
                add(new Bin(key, map));
            }
        } else {
            add(new Bin(key, Value.get(value)));
        }
    }

    public void add(Bin bin) {
        this.bins.add(bin);
    }

    public void setKeyForWrite(Optional<Key> newKey) {
        newKey.ifPresent(value -> this.key = value);
    }
}
