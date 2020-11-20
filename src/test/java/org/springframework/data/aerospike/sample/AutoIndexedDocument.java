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

    @Indexed(type = IndexType.NUMERIC, name = "precreated_index")
    Integer preCreatedIndex;

    String nonIndexed;

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