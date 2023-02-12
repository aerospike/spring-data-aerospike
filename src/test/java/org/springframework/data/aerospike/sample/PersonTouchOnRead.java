package org.springframework.data.aerospike.sample;

import lombok.experimental.SuperBuilder;
import org.springframework.data.aerospike.mapping.Document;

@SuperBuilder
@Document(touchOnRead = true)
public class PersonTouchOnRead extends Person {

}
