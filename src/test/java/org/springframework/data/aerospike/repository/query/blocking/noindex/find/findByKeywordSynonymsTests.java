package org.springframework.data.aerospike.repository.query.blocking.noindex.find;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.noindex.PersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Person;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class findByKeywordSynonymsTests extends PersonRepositoryQueryTests {

    @Test
    void findBySimplePropertyEquals_String() {
        List<Person> resultFind = repository.findByFirstName("Leroi");
        assertThat(resultFind).containsOnly(leroi, leroi2);

        List<Person> resultRead = repository.readByFirstName("Leroi");
        assertThat(resultRead).containsExactlyInAnyOrderElementsOf(resultFind);

        List<Person> resultGet = repository.getByFirstName("Leroi");
        assertThat(resultGet).containsExactlyInAnyOrderElementsOf(resultFind);

        List<Person> resultQuery = repository.queryByFirstName("Leroi");
        assertThat(resultQuery).containsExactlyInAnyOrderElementsOf(resultFind);

        List<Person> resultSearch = repository.searchByFirstName("Leroi");
        assertThat(resultSearch).containsExactlyInAnyOrderElementsOf(resultFind);

        List<Person> resultStream = repository.streamByFirstName("Leroi");
        assertThat(resultStream).containsExactlyInAnyOrderElementsOf(resultFind);
    }

}
