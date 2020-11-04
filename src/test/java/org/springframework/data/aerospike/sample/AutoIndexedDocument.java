package org.springframework.data.aerospike.sample;

import com.aerospike.client.query.IndexType;
import lombok.Value;
import org.springframework.data.aerospike.index.Indexed;
import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.aerospike.mapping.Field;
import org.springframework.data.annotation.Id;

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
}