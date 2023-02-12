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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/**
 * Value object to carry data to be written in object conversion.
 *
 * @author Oliver Gierke
 * @author Anastasiia Smirnova
 */
public class AerospikeWriteData {

    private Key key;
    private Collection<Bin> bins;
    private int expiration;
    private Integer version;
    private Collection<String> requestedBins;

    /**
     * Use the other constructor.
     */
    @Deprecated
    public AerospikeWriteData(Key key, Collection<Bin> bins, int expiration) {
        this(key, bins, expiration, null);
    }

    public AerospikeWriteData(Key key, Collection<Bin> bins, int expiration, Integer version) {
        this(key, bins, expiration, version, Collections.emptyList());
    }

    public AerospikeWriteData(Key key, Collection<Bin> bins, int expiration, Integer version,
                              Collection<String> requestedBins) {
        this.key = key;
        this.bins = bins;
        this.expiration = expiration;
        this.version = version;
        this.requestedBins = requestedBins;
    }

    public static AerospikeWriteData forWrite(String namespace) {
        return new AerospikeWriteData(new Key(namespace, "", ""), new ArrayList<>(), 0, null);
    }

    public Key getKey() {
        return key;
    }

    public void setKey(Key key) {
        this.key = key;
    }

    public Collection<Bin> getBins() {
        return Collections.unmodifiableCollection(bins);
    }

    public void setBins(Collection<Bin> bins) {
        this.bins = bins;
    }

    public Bin[] getBinsAsArray() {
        return bins.toArray(new Bin[0]);
    }

    public void addBin(String key, Object value) {
        add(new Bin(key, value));
    }

    public void add(Bin bin) {
        this.bins.add(bin);
    }

    public int getExpiration() {
        return expiration;
    }

    public void setExpiration(int expiration) {
        this.expiration = expiration;
    }

    public Optional<Integer> getVersion() {
        return Optional.ofNullable(version);
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public Collection<String> getRequestedBins() {
        return requestedBins;
    }

    public void setRequestedBins(Collection<String> requestedBins) {
        this.requestedBins = requestedBins;
    }
}
