/*
 * Copyright 2012-2021 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 		https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.sample;

import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import lombok.Value;
import org.springframework.data.aerospike.annotation.Indexed;
import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.aerospike.mapping.Field;
import org.springframework.data.annotation.Id;

import java.util.List;
import java.util.Map;

@Value
@Document(collection = "auto-indexed-set")
public class AutoIndexedDocument {

    @Id
    String id;

    @Indexed(type = IndexType.STRING)
    String someField;

    @Indexed(type = IndexType.STRING)
    @Field("shortName")
    String someLongNamedField;

    @Indexed(type = IndexType.NUMERIC, name = "overridden_index_name")
    Integer customIndexName;

    @Indexed(type = IndexType.NUMERIC, name = "pre_created_index")
    Integer preCreatedIndex;

    String nonIndexed;

    @Indexed(type = IndexType.STRING, name = "placeholder_" + "${indexSuffix}")
    String placeHolderIdx;

    @Indexed(type = IndexType.STRING, collectionType = IndexCollectionType.LIST)
    List<String> listOfStrings;

    @Indexed(type = IndexType.NUMERIC, collectionType = IndexCollectionType.LIST)
    List<Integer> listOfInts;

    @Indexed(type = IndexType.STRING, collectionType = IndexCollectionType.MAPKEYS)
    Map<String, String> mapOfStrKeys;

    @Indexed(type = IndexType.STRING, collectionType = IndexCollectionType.MAPVALUES)
    Map<String, String> mapOfStrVals;

    @Indexed(type = IndexType.NUMERIC, collectionType = IndexCollectionType.MAPKEYS)
    Map<String, String> mapOfIntKeys;

    @Indexed(type = IndexType.NUMERIC, collectionType = IndexCollectionType.MAPVALUES)
    Map<String, String> mapOfIntVals;
}