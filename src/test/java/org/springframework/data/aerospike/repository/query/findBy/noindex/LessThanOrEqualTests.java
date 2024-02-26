package org.springframework.data.aerospike.repository.query.findBy.noindex;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.utility.TestUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is less than or equal" repository query. Keywords: LessThanEqual, IsLessThanEqual.
 */
public class LessThanOrEqualTests extends PersonRepositoryQueryTests {

    @Test
    void findByNestedIntegerSimplePropertyLessThanOrEqual() {
        alicia.setFriend(boyd);
        repository.save(alicia);
        dave.setFriend(oliver);
        repository.save(dave);
        carter.setFriend(dave);
        repository.save(carter);
        leroi.setFriend(carter);
        repository.save(leroi);

        List<Person> result = repository.findByFriendAgeLessThanEqual(42);

        assertThat(result)
            .hasSize(2)
            .containsExactlyInAnyOrder(dave, carter);

        TestUtils.setFriendsToNull(repository, alicia, dave, carter, leroi);
    }

    @Test
    void findByCollectionValueLessThanOrEqual_Number() {
        List<Person> persons;
        persons = repository.findByIntsLessThanEqual(500);
        assertThat(persons).containsOnly(oliver);

        persons = repository.findByIntsLessThanEqual(Long.MAX_VALUE - 1);
        assertThat(persons).containsOnly(oliver, alicia);

        persons = repository.findByIntsLessThanEqual(Long.MAX_VALUE);
        assertThat(persons).containsOnly(oliver, alicia);
    }

    @Test
    void findByCollectionValueLessThanOrEqual_String() {
        List<Person> persons;
        persons = repository.findByStringsLessThanEqual("str4");
        assertThat(persons).containsOnly(dave, donny);

        persons = repository.findByStringsLessThanEqual("str3");
        assertThat(persons).containsOnly(dave, donny);

        persons = repository.findByStringsLessThanEqual("str2");
        assertThat(persons).containsOnly(dave, donny);

        persons = repository.findByStringsLessThanEqual("str0");
        assertThat(persons).containsOnly(dave);

        persons = repository.findByStringsLessThanEqual("str");
        assertThat(persons).isEmpty();
    }

    @Test
    void findByExactMapKeyWithValueLessThanOrEqual() {
        assertThat(carter.getIntMap()).containsKey("key2");
        assertThat(carter.getIntMap().get("key2") > 0).isTrue();
        assertThat(carter.getIntMap().get("key2") <= 1).isTrue();

        List<Person> persons = repository.findByIntMapLessThanEqual("key2", 1);
        assertThat(persons).containsExactly(carter);

        if (template.getAerospikeConverter().getAerospikeSettings().isKeepOriginalKeyTypes()) {
            carter.setLongIntMap(Map.of(10L, 10));
            repository.save(carter);
            assertThat(carter.getLongIntMap().get(10L) <= 10).isTrue();
            List<Person> persons2 = repository.findByLongIntMapLessThanEqual(10L, 10);
            assertThat(persons2).containsExactly(carter);
            carter.setLongIntMap(null); // cleanup
            repository.save(carter);

            carter.setDoubleIntMap(Map.of(0.9D, 10));
            repository.save(carter);
            assertThat(carter.getDoubleIntMap().get(0.9D) <= 10).isTrue();
            List<Person> persons3 = repository.findByDoubleIntMapLessThanEqual(0.9D, 10);
            assertThat(persons3).containsExactly(carter);
            carter.setDoubleIntMap(null); // cleanup
            repository.save(carter);

            byte[] byteArray = new byte[]{0, 1, 1, 0};
            carter.setByteArrayIntMap(Map.of(byteArray, 10));
            repository.save(carter);
            assertThat(carter.getByteArrayIntMap().get(byteArray) <= 10).isTrue();
            List<Person> persons4 = repository.findByByteArrayIntMapLessThanEqual(byteArray, 10);
            assertThat(persons4).hasSize(1);
            byte[] byteArrayIntMapFirstKey = persons4.get(0).getByteArrayIntMap().keySet().iterator().next();
            assertThat(Arrays.equals(byteArrayIntMapFirstKey, byteArray)).isTrue();
            carter.setByteArrayIntMap(null); // cleanup
            repository.save(carter);
        }
    }
}
