package org.springframework.data.aerospike.core.reactive;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import org.junit.Test;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.aerospike.AsyncUtils;
import org.springframework.data.aerospike.SampleClasses;
import org.springframework.data.aerospike.core.Person;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

public class ReactiveAerospikeTemplateInsertTests extends BaseReactiveAerospikeTemplateTests {

	@Test
	public void insertsAndFindsWithCustomCollectionSet() {
		SampleClasses.CustomCollectionClass initial = new SampleClasses.CustomCollectionClass(id, "data0");
		reactiveTemplate.insert(initial).block();

		Record record = client.get(new Policy(), new Key(getNameSpace(), "custom-set", id));

		assertThat(record.getString("data")).isEqualTo("data0");
		assertThat(findById(id, SampleClasses.CustomCollectionClass.class)).isEqualTo(initial);
	}

	@Test
	public void insertsDocumentWithListMapDateStringLongValues() {
		Map<String, String> map = Collections.singletonMap("k", "v");
		Person friend = new Person(nextId(), "Anna", 43);
		List<String> list = Arrays.asList("a", "b", "c");
		String email = "dave@gmail.com";
		Date dateOfBirth = new Date();
		Person customer = new Person(id, "Dave", 45, map, friend, true, dateOfBirth, list, email);

		reactiveTemplate.insert(customer).block();

		Person actual = findById(id, Person.class);
		assertThat(actual.getId()).isEqualTo(id);
		assertThat(actual.getAge()).isEqualTo(45);
		assertThat(actual.getFirstName()).isEqualTo("Dave");
		assertThat(actual.getMap()).isEqualTo(map);
		assertThat(actual.getFriend()).isEqualToIgnoringGivenFields(friend, "id");//metadata fields are not saved for internal structures
		assertThat(actual.isActive()).isTrue();
		assertThat(actual.getDateOfBirth()).isEqualTo(dateOfBirth);
		assertThat(actual.getList()).isEqualTo(list);
		assertThat(actual.getEmailAddress()).isEqualTo(email);
	}

	@Test
	public void insertsAndFindsDocumentWithByteArrayField() {
		SampleClasses.DocumentWithByteArray document = new SampleClasses.DocumentWithByteArray(id, new byte[]{1, 0, 0, 1, 1, 1, 0, 0});

		reactiveTemplate.insert(document).block();

		SampleClasses.DocumentWithByteArray result = findById(id, SampleClasses.DocumentWithByteArray.class);
		assertThat(result).isEqualTo(document);
	}

	@Test
	public void insertsDocumentWithNullFields() {
		SampleClasses.VersionedClass document = new SampleClasses.VersionedClass(id, null);

		reactiveTemplate.insert(document).block();

		assertThat(document.getField()).isNull();
	}

	@Test
	public void insertsDocumentWithZeroVersionIfThereIsNoDocumentWithSameKey() {
		SampleClasses.VersionedClass document = new SampleClasses.VersionedClass(id, "any", 0);

		reactiveTemplate.insert(document).block();

		assertThat(document.getVersion()).isEqualTo(1);
	}

	@Test
	public void insertsDocumentWithVersionGreaterThanZeroIfThereIsNoDocumentWithSameKey() {
		SampleClasses.VersionedClass document = new SampleClasses.VersionedClass(id, "any", 5);

		reactiveTemplate.insert(document).block();

		assertThat(document.getVersion()).isEqualTo(1);
	}

	@Test
	public void throwsExceptionForDuplicateId() {
		Person person = new Person(id, "Amol", 28);

		reactiveTemplate.insert(person).block();
		StepVerifier.create(reactiveTemplate.insert(person))
				.expectError(DuplicateKeyException.class)
				.verify();
	}

	@Test
	public void throwsExceptionForDuplicateIdForVersionedDocument() {
		SampleClasses.VersionedClass document = new SampleClasses.VersionedClass(id, "any", 5);

		reactiveTemplate.insert(document).block();
		StepVerifier.create(reactiveTemplate.insert(document))
				.expectError(DuplicateKeyException.class)
				.verify();
	}

	@Test
	public void insertsOnlyFirstDocumentAndNextAttemptsShouldFailWithDuplicateKeyExceptionForVersionedDocument() throws Exception {
		AtomicLong counter = new AtomicLong();
		AtomicLong duplicateKeyCounter = new AtomicLong();
		int numberOfConcurrentSaves = 5;

		AsyncUtils.executeConcurrently(numberOfConcurrentSaves, () -> {
			long counterValue = counter.incrementAndGet();
			String data = "value-" + counterValue;
			reactiveTemplate.insert(new SampleClasses.VersionedClass(id, data))
					.onErrorResume(DuplicateKeyException.class, e -> {
						duplicateKeyCounter.incrementAndGet();
						return Mono.empty();
					})
					.block();
		});

		assertThat(duplicateKeyCounter.intValue()).isEqualTo(numberOfConcurrentSaves - 1);
	}

	@Test
	public void insertsOnlyFirstDocumentAndNextAttemptsShouldFailWithDuplicateKeyExceptionForNonVersionedDocument() throws Exception {
		AtomicLong counter = new AtomicLong();
		AtomicLong duplicateKeyCounter = new AtomicLong();
		int numberOfConcurrentSaves = 5;

		AsyncUtils.executeConcurrently(numberOfConcurrentSaves, () -> {
			long counterValue = counter.incrementAndGet();
			String data = "value-" + counterValue;
			reactiveTemplate.insert(new Person(id, data, 28))
					.onErrorResume(DuplicateKeyException.class, e -> {
						duplicateKeyCounter.incrementAndGet();
						return Mono.empty();
					})
					.block();
		});

		assertThat(duplicateKeyCounter.intValue()).isEqualTo(numberOfConcurrentSaves - 1);

	}
}
