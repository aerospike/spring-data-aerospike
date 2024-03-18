package org.springframework.data.aerospike.repository.query.findBy.noindex;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.PersonSomeFields;
import org.springframework.data.aerospike.utility.TestUtils;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the "Is greater than" repository query. Keywords: GreaterThan, IsGreaterThan.
 */
public class GreaterThanTests extends PersonRepositoryQueryTests {

    @Test
    void findBySimpleProperty_Integer_Paginated() {
        Slice<Person> slice = repository.findByAgeGreaterThan(40, PageRequest.of(0, 10));
        assertThat(slice.hasContent()).isTrue();
        assertThat(slice.hasNext()).isFalse();
        assertThat(slice.getContent()).hasSize(4).contains(dave, carter, boyd, leroi);

        Slice<Person> slice2 = repository.findByAgeGreaterThan(40, PageRequest.of(0, 1));
        assertThat(slice2.hasContent()).isTrue();
        assertThat(slice2.hasNext()).isTrue();
        assertThat(slice2.getContent()).containsAnyOf(dave, carter, boyd, leroi).hasSize(1);

        Slice<Person> slice3 = repository.findByAgeGreaterThan(100, PageRequest.of(0, 10));
        assertThat(slice3.hasContent()).isFalse();
        assertThat(slice3.hasNext()).isFalse();
        assertThat(slice3.getContent()).isEmpty();
    }

    @Test
    void findBySimpleProperty_Integer_PaginatedHasPrevHasNext() {
        Slice<Person> first = repository.findByAgeGreaterThan(40, PageRequest.of(0, 1, Sort.by("age")));

        assertThat(first.hasContent()).isTrue();
        assertThat(first.getNumberOfElements()).isEqualTo(1);
        assertThat(first.hasNext()).isTrue();
        assertThat(first.isFirst()).isTrue();
        assertThat(first.isLast()).isFalse();

        Slice<Person> last = repository.findByAgeGreaterThan(20, PageRequest.of(4, 2, Sort.by("age")));
        assertThat(last.hasContent()).isTrue();
        assertThat(last.getNumberOfElements()).isEqualTo(2);
        assertThat(last.hasNext()).isFalse();
        assertThat(last.isLast()).isTrue();
    }

    @Test
    void findBySimpleProperty_Integer_SortedWithOffset() {
        List<Person> result = IntStream.range(0, 4)
            .mapToObj(index -> repository.findByAgeGreaterThan(40, PageRequest.of(
                index, 1, Sort.by("age")
            )))
            .flatMap(slice -> slice.getContent().stream())
            .collect(Collectors.toList());

        assertThat(result)
            .hasSize(4)
            .containsSequence(dave, leroi, boyd, carter);
    }

    @Test
    void findBySimpleProperty_Integer_UnsortedWithOffset_NegativeTest() {
        assertThatThrownBy(() -> repository.findByAgeGreaterThan(1, PageRequest.of(1, 2)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unsorted query must not have offset value. For retrieving paged results use sorted query.");
    }

    @Test
    void findBySimpleProperty_Integer_Unpaged() {
        Slice<Person> slice = repository.findByAgeGreaterThan(40, Pageable.unpaged());
        assertThat(slice.hasContent()).isTrue();
        assertThat(slice.getNumberOfElements()).isGreaterThan(0);
        assertThat(slice.hasNext()).isFalse();
        assertThat(slice.isLast()).isTrue();
    }

    @Test
    void findByNestedSimplePropertyGreaterThan_Integer() {
        alicia.setFriend(boyd);
        repository.save(alicia);
        dave.setFriend(oliver);
        repository.save(dave);
        carter.setFriend(dave);
        repository.save(carter);
        leroi.setFriend(carter);
        repository.save(leroi);

        assertThat(alicia.getFriend().getAge()).isGreaterThan(42);
        assertThat(leroi.getFriend().getAge()).isGreaterThan(42);

        List<Person> result = repository.findByFriendAgeGreaterThan(42);

        assertThat(result)
            .hasSize(2)
            .containsExactlyInAnyOrder(alicia, leroi);

        TestUtils.setFriendsToNull(repository, alicia, dave, carter, leroi);
    }

    @Test
    void findBySimplePropertyGreaterThan_Integer_projection() {
        Slice<PersonSomeFields> slice = repository.findPersonSomeFieldsByAgeGreaterThan(40, PageRequest.of(0, 10));

        assertThat(slice.hasContent()).isTrue();
        assertThat(slice.hasNext()).isFalse();
        assertThat(slice.getContent()).hasSize(4).contains(dave.toPersonSomeFields(),
            carter.toPersonSomeFields(), boyd.toPersonSomeFields(), leroi.toPersonSomeFields());
    }

    @Test
    void findBySimplePropertyGreaterThan_String() {
        List<Person> result = repository.findByFirstNameGreaterThan("Leroa");
        assertThat(result).contains(leroi, leroi2);
    }

    @Test
    void findByCollectionGreaterThan() {
        List<Integer> listToCompare1 = List.of(100, 200, 300, 400);
        List<Integer> listToCompare2 = List.of(425, 550);
        List<Integer> listToCompare3 = List.of(426, 551, 991);
        List<Integer> listToCompare4 = List.of(1000, 2000, 3000, 4000);
        List<Integer> listToCompare5 = List.of(551, 601, 991);
        List<Integer> listToCompare6 = List.of(550, 600, 990);

        List<Person> persons;
        persons = repository.findByIntsGreaterThan(listToCompare1);
        assertThat(persons).containsOnly(oliver, alicia);

        persons = repository.findByIntsGreaterThan(listToCompare2);
        assertThat(persons).containsOnly(oliver, alicia);

        persons = repository.findByIntsGreaterThan(listToCompare3);
        assertThat(persons).containsOnly(alicia);

        persons = repository.findByIntsGreaterThan(listToCompare4);
        assertThat(persons).isEmpty();

        persons = repository.findByIntsGreaterThan(listToCompare5);
        assertThat(persons).isEmpty();

        persons = repository.findByIntsGreaterThan(listToCompare6);
        assertThat(persons).isEmpty();
    }

    /*
        Note:
        only the upper level ListOfLists will be compared even if the parameter has different number of levels
        So findByListOfListsGreaterThan(List.of(1)) and findByListOfListsGreaterThan(List.of(List.of(List.of(1))))
        will compare with the given parameter only the upper level ListOfLists itself
     */
    @Test
    void findByCollectionOfListsGreaterThan() {
        List<List<Integer>> listOfLists1 = List.of(List.of(100));
        List<List<Integer>> listOfLists2 = List.of(List.of(101));
        List<List<Integer>> listOfLists3 = List.of(List.of(102));
        List<List<Integer>> listOfLists4 = List.of(List.of(1000));
        stefan.setListOfIntLists(listOfLists1);
        repository.save(stefan);
        douglas.setListOfIntLists(listOfLists2);
        repository.save(douglas);
        matias.setListOfIntLists(listOfLists3);
        repository.save(matias);
        leroi2.setListOfIntLists(listOfLists4);
        repository.save(leroi2);

        List<Person> persons;
        persons = repository.findByListOfIntListsGreaterThan(List.of(List.of(99)));
        assertThat(persons).containsOnly(stefan, douglas, matias, leroi2);

        persons = repository.findByListOfIntListsGreaterThan(List.of(List.of(100)));
        assertThat(persons).containsOnly(douglas, matias, leroi2);

        persons = repository.findByListOfIntListsGreaterThan(List.of(List.of(102)));
        assertThat(persons).containsOnly(leroi2);

        persons = repository.findByListOfIntListsGreaterThan(List.of(List.of(401)));
        assertThat(persons).containsOnly(leroi2);

        persons = repository.findByListOfIntListsGreaterThan(List.of(List.of(4000)));
        assertThat(persons).isEmpty();
    }

    @Test
    void findByCollection_NegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByIntsGreaterThan(100, 200))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.ints GT: invalid number of arguments, expecting one");

        assertThatThrownBy(() -> negativeTestsRepository.findByIntsGreaterThan(100))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.ints GT: invalid argument type, expecting Collection");
    }

    @Test
    void findByMapGreaterThan() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            assertThat(boyd.getStringMap()).isNotEmpty();
            assertThat(donny.getStringMap()).isNotEmpty();

            Map<String, String> mapToCompare = Map.of("Key", "Val", "Key2", "Val2");
            List<Person> persons = repository.findByStringMapGreaterThan(mapToCompare);
            assertThat(persons).containsExactlyInAnyOrder(boyd);
        }
    }
}
