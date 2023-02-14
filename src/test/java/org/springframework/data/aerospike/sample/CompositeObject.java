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

import lombok.Builder;
import lombok.Value;
import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceCreator;

@Builder
@Value
@Document
public class CompositeObject {

    @Id
    String id;
    int intValue;
    SimpleObject simpleObject;

    @PersistenceCreator
    public CompositeObject(String id, int intValue, SimpleObject simpleObject) {
        this.id = id;
        this.intValue = intValue;
        this.simpleObject = simpleObject;
    }
}
