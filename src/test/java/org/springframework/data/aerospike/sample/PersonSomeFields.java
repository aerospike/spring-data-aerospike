package org.springframework.data.aerospike.sample;

import lombok.Data;
import org.springframework.data.aerospike.mapping.Field;

@Data
public class PersonSomeFields {
    private String firstName;
    private String lastName;
    @Field("email")
    private String emailAddress;
}
