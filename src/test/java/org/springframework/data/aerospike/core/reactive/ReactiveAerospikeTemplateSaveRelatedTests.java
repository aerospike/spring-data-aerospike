package org.springframework.data.aerospike.core.reactive;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import org.junit.Test;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.aerospike.AsyncUtils;
import org.springframework.data.aerospike.SampleClasses;
import org.springframework.data.aerospike.SampleClasses.VersionedClass;
import org.springframework.data.aerospike.core.Person;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for save related methods in {@link ReactiveAerospikeTemplate}.
 *
 * @author Igor Ermolenko
 */
public class ReactiveAerospikeTemplateSaveRelatedTests extends BaseReactiveAerospikeTemplateTests {

    @Test
    public void save_shouldSaveAndSetVersion() {
        VersionedClass first = new VersionedClass(id, "foo");
        reactiveTemplate.save(first).block();

        assertThat(first.version).isEqualTo(1);
        assertThat(findById(id, VersionedClass.class).version).isEqualTo(1);
    }

    @Test(expected = OptimisticLockingFailureException.class)
    public void save_shouldNotSaveDocumentIfItAlreadyExistsWithZeroVersion() {
        reactiveTemplate.save(new VersionedClass(id, "foo", 0)).block();
        reactiveTemplate.save(new VersionedClass(id, "foo", 0)).block();
    }

    @Test
    public void save_shouldSaveDocumentWithEqualVersion() {
        reactiveTemplate.save(new VersionedClass(id, "foo", 0)).block();

        reactiveTemplate.save(new VersionedClass(id, "foo", 1)).block();
        reactiveTemplate.save(new VersionedClass(id, "foo", 2)).block();
    }

    @Test(expected = DataRetrievalFailureException.class)
    public void save_shouldFailSaveNewDocumentWithVersionGreaterThanZero() {
        reactiveTemplate.save(new VersionedClass(id, "foo", 5)).block();
    }

    @Test
    public void save_shouldUpdateNullField() {
        VersionedClass versionedClass = new VersionedClass(id, null, 0);
        VersionedClass saved = reactiveTemplate.save(versionedClass).block();
        reactiveTemplate.save(saved).block();
    }

    @Test
    public void save_shouldUpdateNullFieldForClassWithVersionField() {
        VersionedClass versionedClass = new VersionedClass(id, "field", 0);
        reactiveTemplate.save(versionedClass).block();

        assertThat(findById(id, VersionedClass.class).getField()).isEqualTo("field");

        versionedClass.setField(null);
        reactiveTemplate.save(versionedClass).block();

        assertThat(findById(id, VersionedClass.class).getField()).isNull();
    }

    @Test
    public void save_shouldUpdateNullFieldForClassWithoutVersionField() {
        Person person = new Person(id, "Oliver");
        reactiveTemplate.save(person).block();

        assertThat(findById(id, Person.class).getFirstName()).isEqualTo("Oliver");

        person.setFirstName(null);
        reactiveTemplate.save(person).block();

        assertThat(findById(id, Person.class).getFirstName()).isNull();
    }

    @Test
    public void save_shouldUpdateExistingDocument() {
        VersionedClass one = new VersionedClass(id, "foo", 0);
        reactiveTemplate.save(one).block();

        reactiveTemplate.save(new VersionedClass(id, "foo1", one.version)).block();

        VersionedClass value = findById(id, VersionedClass.class);
        assertThat(value.version).isEqualTo(2);
        assertThat(value.field).isEqualTo("foo1");
    }

    @Test
    public void save_shouldSetVersionWhenSavingTheSameDocument() {
        VersionedClass one = new VersionedClass(id, "foo");
        reactiveTemplate.save(one).block();
        reactiveTemplate.save(one).block();
        reactiveTemplate.save(one).block();

        assertThat(one.version).isEqualTo(3);
    }

    @Test
    public void save_shouldUpdateAlreadyExistingDocument() throws Exception {
        AtomicLong counter = new AtomicLong();
        int numberOfConcurrentSaves = 5;

        VersionedClass initial = new VersionedClass(id, "value-0");
        reactiveTemplate.save(initial).block();
        assertThat(initial.version).isEqualTo(1);

        AsyncUtils.executeConcurrently(numberOfConcurrentSaves, () -> {
            boolean saved = false;
            while (!saved) {
                long counterValue = counter.incrementAndGet();
                VersionedClass messageData = findById(id, VersionedClass.class);
                messageData.field = "value-" + counterValue;
                try {
                    reactiveTemplate.save(messageData).block();
                    saved = true;
                } catch (OptimisticLockingFailureException ignore) {
                }
            }
        });

        VersionedClass actual = findById(id, VersionedClass.class);

        assertThat(actual.field).isNotEqualTo(initial.field);
        assertThat(actual.version).isNotEqualTo(initial.version);
        assertThat(actual.version).isEqualTo(initial.version + numberOfConcurrentSaves);
    }

    @Test
    public void save_shouldSaveOnlyFirstDocumentAndNextAttemptsShouldFailWithOptimisticLockingException() throws Exception {
        AtomicLong counter = new AtomicLong();
        AtomicLong optimisticLockCounter = new AtomicLong();
        int numberOfConcurrentSaves = 5;

        AsyncUtils.executeConcurrently(numberOfConcurrentSaves, () -> {
            long counterValue = counter.incrementAndGet();
            String data = "value-" + counterValue;
            VersionedClass messageData = new VersionedClass(id, data);
            reactiveTemplate.save(messageData)
                    .onErrorResume(OptimisticLockingFailureException.class, (e) -> {
                        optimisticLockCounter.incrementAndGet();
                        return Mono.empty();
                    })
                    .block();
        });

        assertThat(optimisticLockCounter.intValue()).isEqualTo(numberOfConcurrentSaves - 1);
    }

    @Test
    public void save_shouldSaveMultipleTimeDocumentWithoutVersion() {
        SampleClasses.CustomCollectionClass one = new SampleClasses.CustomCollectionClass(id, "numbers");

        reactiveTemplate.save(one).block();
        reactiveTemplate.save(one).block();

        assertThat(findById(id, SampleClasses.CustomCollectionClass.class)).isEqualTo(one);
    }

    @Test
    public void save_shouldUpdateDocumentDataWithoutVersion() {
        SampleClasses.CustomCollectionClass first = new SampleClasses.CustomCollectionClass(id, "numbers");
        SampleClasses.CustomCollectionClass second = new SampleClasses.CustomCollectionClass(id, "hot dog");

        reactiveTemplate.save(first).block();
        reactiveTemplate.save(second).block();

        assertThat(findById(id, SampleClasses.CustomCollectionClass.class)).isEqualTo(second);
    }

    @Test
    public void save_shouldReplaceAllBinsPresentInAerospikeWhenSavingDocument() {
        Key key = new Key(getNameSpace(), "versioned-set", id);
        VersionedClass first = new VersionedClass(id, "foo");
        reactiveTemplate.save(first).block();
        addNewFieldToSavedDataInAerospike(key);

        reactiveTemplate.save(new VersionedClass(id, "foo2", 2)).block();

        Record record2 = client.get(new Policy(), key);
        assertThat(record2.bins.get("notPresent")).isNull();
        assertThat(record2.bins.get("field")).isEqualTo("foo2");
    }

    @Test(expected = IllegalArgumentException.class)
    public void save_rejectsNullObjectToBeSaved() {
        reactiveTemplate.save(null).block();
    }

    @Test
    public void insertAll_shouldInsertAllDocuments() {
        Person customer1 = new Person("dave-002", "Dave");
        Person customer2 = new Person("james-007", "James");
        reactiveTemplate.insertAll(asList(customer1, customer2)).blockLast();
        assertThat(findById("dave-002", Person.class)).isEqualTo(customer1);
        assertThat(findById("james-007", Person.class)).isEqualTo(customer2);
    }

    @Test(expected = DuplicateKeyException.class)
    public void insertAll_rejectsDuplicateId() {
        Person person = new Person(id, "Amol");
        person.setAge(28);
        reactiveTemplate.insertAll(asList(person, person)).blockLast();
    }

    @Test
    public void insert_insertsSimpleEntityCorrectly() {
        Person person = new Person(id,"Oliver");
        person.setAge(25);
        reactiveTemplate.insert(person).block();

        Person person1 =  findById(id, Person.class);
        assertThat(person1).isEqualTo(person);
    }

    @Test(expected = DuplicateKeyException.class)
    public void insert_throwsExceptionForDuplicateIds() {
        Person person = new Person(id,"Amol");
        person.setAge(28);

        reactiveTemplate.insert(person).block();
        reactiveTemplate.insert(person).block();
    }

    @Test
    public void update_shouldUpdateDocumentCorrectly(){
        Person person = new Person(id,"WLastName",5);
        reactiveTemplate.insert(person).block();
        person.setAge(11);
        reactiveTemplate.update(person).block();

        Person result = findById(id, Person.class);
        assertThat(result.getAge()).isEqualTo(11);
    }

    @Test(expected = DataRetrievalFailureException.class)
    public void update_shouldThrowExceptionOnUpdateForNonexistingKey() {
        reactiveTemplate.update(new Person(id, "svenfirstName", 11)).block();
    }

    @Test
    public void update_shouldUpdateDocumentWithListField() {
        Person personSven02 = new Person(id, "QLastName", 50);
        List<String> list = asList("string1", "string2","string3");
        personSven02.setList(list);
        reactiveTemplate.insert(personSven02).block();

        Person personWithList = findById(id, Person.class);
        personWithList.getList().add("Added something new");
        reactiveTemplate.update(personWithList).block();
        Person personWithList2 = findById(id, Person.class);

        assertThat(personWithList2).isEqualTo(personWithList);
        assertThat(personWithList2.getList()).hasSize(4);
    }

    @Test
    public void update_shouldUpdateDocumentWithMapField() {
        Person personSven02 = new Person(id, "QLastName", 50);
        Map<String, String> map = new HashMap<String, String>();
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        personSven02.setMap(map);
        reactiveTemplate.insert(personSven02).block();

        Person personWithList = findById(id, Person.class);
        personWithList.getMap().put("key4","Added something new");
        reactiveTemplate.update(personWithList).block();
        Person personWithList2 = findById(id, Person.class);

        assertThat(personWithList2).isEqualTo(personWithList);
        assertThat(personWithList2.getMap()).hasSize(4);
        assertThat(personWithList2.getMap().get("key4")).isEqualTo("Added something new");
    }
}
