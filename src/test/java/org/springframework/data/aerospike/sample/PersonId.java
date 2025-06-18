package org.springframework.data.aerospike.sample;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.aerospike.mapping.Field;
import org.springframework.data.annotation.Id;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PersonId {

    @Id
    private String id;
}
