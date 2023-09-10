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

import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.RecordSet;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Iterator for traversing a collection of KeyRecords
 *
 * @author peter
 */
public class KeyRecordIterator implements Iterator<KeyRecord>, Closeable {

    private final String namespace;
    private final Integer closeLock = 0;
    private RecordSet recordSet;
    private Iterator<KeyRecord> recordSetIterator;
    private KeyRecord singleRecord;

    public KeyRecordIterator(String namespace) {
        super();
        this.namespace = namespace;
    }

    public KeyRecordIterator(String namespace, KeyRecord singleRecord) {
        this(namespace);
        this.singleRecord = singleRecord;
    }

    public KeyRecordIterator(String namespace, RecordSet recordSet) {
        this(namespace);
        this.recordSet = recordSet;
        this.recordSetIterator = recordSet.iterator();
    }

    @Override
    public void close() {
        //noinspection synchronization
        synchronized (closeLock) {
            if (recordSet != null)
                recordSet.close();
            if (singleRecord != null)
                singleRecord = null;
        }
    }

    @Override
    public boolean hasNext() {
        if (this.recordSetIterator != null)
            return this.recordSetIterator.hasNext();
        else return this.singleRecord != null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public KeyRecord next() {
        KeyRecord keyRecord = null;

        if (this.recordSetIterator != null) {
            keyRecord = this.recordSetIterator.next();
        } else if (singleRecord != null) {
            keyRecord = singleRecord;
            singleRecord = null;
        }
        return keyRecord;
    }

    @Override
    public void remove() {
    }

    @Override
    public String toString() {
        return this.namespace;
    }
}
