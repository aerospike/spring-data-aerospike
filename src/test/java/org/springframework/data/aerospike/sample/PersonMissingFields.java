package org.springframework.data.aerospike.sample;

import lombok.Data;

@Data
public class PersonMissingFields {
    private String firstName;
    private String lastName;
    private String missingField;
}
