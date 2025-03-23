package org.springframework.data.aerospike.sample;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.aerospike.mapping.Field;
import org.springframework.data.annotation.Id;


@Data
@Builder
public class PersonId {

    @Id
    private String id;
}
