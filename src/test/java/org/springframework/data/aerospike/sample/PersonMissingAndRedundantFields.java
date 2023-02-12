package org.springframework.data.aerospike.sample;

import lombok.Data;

@Data
public class PersonMissingAndRedundantFields {

    private String firstName;
    private String lastName;
    private String missingField;
    // Not annotated with @Field("email") therefore should not be recognized as target field.
    private String emailAddress;
}
