package org.springframework.data.aerospike.config;

import com.aerospike.client.query.IndexType;
import lombok.Value;
import org.springframework.data.aerospike.annotation.Indexed;
import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.annotation.Id;

@Value
@Document(collection = "config-package-document-set")
public class ConfigPackageDocument {

    @Id
    String id;

    @Indexed(type = IndexType.STRING, name = "config-package-document-index")
    String indexedField;
}
