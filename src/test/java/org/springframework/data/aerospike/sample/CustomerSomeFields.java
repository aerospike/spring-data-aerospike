package org.springframework.data.aerospike.sample;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class CustomerSomeFields {
    private String firstname;
    private String lastname;
}
