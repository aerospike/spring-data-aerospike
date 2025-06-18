package org.springframework.data.aerospike.sample;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.aerospike.mapping.Field;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PersonSomeFields {

    private String firstName;
    private String lastName;
    @Field("email")
    private String emailAddress;
}
