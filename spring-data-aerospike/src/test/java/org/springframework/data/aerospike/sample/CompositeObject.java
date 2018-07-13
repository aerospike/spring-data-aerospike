package org.springframework.data.aerospike.sample;

import lombok.Builder;
import lombok.Value;
import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.aerospike.sample.SimpleObject;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;

@Builder
@Value
@Document
public class CompositeObject {

    @Id
    String id;
    int intValue;
    SimpleObject simpleObject;

    @PersistenceConstructor
    public CompositeObject(String id, int intValue, SimpleObject simpleObject) {
        this.id = id;
        this.intValue = intValue;
        this.simpleObject = simpleObject;
    }

}