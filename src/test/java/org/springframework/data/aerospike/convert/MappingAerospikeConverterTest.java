/*
 * Copyright 2012-2020 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.convert;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import org.assertj.core.data.Offset;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.SampleClasses;
import org.springframework.data.aerospike.SampleClasses.AerospikeReadDataToUserConverter;
import org.springframework.data.aerospike.SampleClasses.CollectionOfObjects;
import org.springframework.data.aerospike.SampleClasses.Contact;
import org.springframework.data.aerospike.SampleClasses.CustomTypeWithCustomTypeImmutable;
import org.springframework.data.aerospike.SampleClasses.DocumentWithByteArray;
import org.springframework.data.aerospike.SampleClasses.DocumentWithDefaultConstructor;
import org.springframework.data.aerospike.SampleClasses.DocumentWithExpirationAnnotation;
import org.springframework.data.aerospike.SampleClasses.DocumentWithExpirationAnnotationAndPersistenceConstructor;
import org.springframework.data.aerospike.SampleClasses.DocumentWithUnixTimeExpiration;
import org.springframework.data.aerospike.SampleClasses.Name;
import org.springframework.data.aerospike.SampleClasses.Person;
import org.springframework.data.aerospike.SampleClasses.User;
import org.springframework.data.aerospike.SampleClasses.UserToAerospikeWriteDataConverter;
import org.springframework.data.aerospike.SampleClasses.VersionedClass;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.AsCollections.list;
import static org.springframework.data.aerospike.AsCollections.of;
import static org.springframework.data.aerospike.AsCollections.set;
import static org.springframework.data.aerospike.SampleClasses.EXPIRATION_ONE_MINUTE;
import static org.springframework.data.aerospike.SampleClasses.EXPIRATION_ONE_SECOND;
import static org.springframework.data.aerospike.SampleClasses.SimpleClass.SIMPLESET;
import static org.springframework.data.aerospike.SampleClasses.User.SIMPLESET3;
import static org.springframework.data.aerospike.assertions.KeyAssert.assertThat;

public class MappingAerospikeConverterTest extends BaseMappingAerospikeConverterTest {

	@Test
	public void readsCollectionOfObjectsToSetByDefault() {
		CollectionOfObjects object = new CollectionOfObjects("my-id", list(new Person(null, set(new SampleClasses.Address(new SampleClasses.Street("Zarichna", 1), 202)))));

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();

		converter.write(object, forWrite);

		AerospikeReadData forRead = AerospikeReadData.forRead(forWrite.getKey(), record(forWrite.getBins()));
		CollectionOfObjects actual = converter.read(CollectionOfObjects.class, forRead);

		assertThat(actual).isEqualTo(
				new CollectionOfObjects("my-id", set(new Person(null, set(new SampleClasses.Address(new SampleClasses.Street("Zarichna", 1), 202))))));
	}

	@Test
	public void shouldReadCustomTypeWithCustomTypeImmutable() {
		Map<String, Object> bins = of("field", of(
				"listOfObjects", ImmutableList.of("firstItem", of("keyInList", "valueInList")),
				"mapWithObjectValue", of("map", of("key", "value"))
		));
		AerospikeReadData forRead = AerospikeReadData.forRead(new Key(NAMESPACE, SIMPLESET, 10L), record(bins));

		CustomTypeWithCustomTypeImmutable actual = converter.read(CustomTypeWithCustomTypeImmutable.class, forRead);

		CustomTypeWithCustomTypeImmutable expected = new CustomTypeWithCustomTypeImmutable(new SampleClasses.ImmutableListAndMap(ImmutableList.of("firstItem", of("keyInList", "valueInList")),
				of("map", of("key", "value"))));
		assertThat(actual).isEqualTo(expected);
	}

	@Test
	public void usesDocumentsStoredTypeIfSubtypeOfRequest() {
		Map<String, Object> bins = of(
				"@_class", Person.class.getName(),
				"addresses", list()
		);
		AerospikeReadData dbObject = AerospikeReadData.forRead(new Key(NAMESPACE, "Person", "kate-01"), record(bins));

		Contact result = converter.read(Contact.class, dbObject);
		assertThat(result).isInstanceOf(Person.class);
	}

	@Test
	public void shouldWriteAndReadUsingCustomConverter() {
		MappingAerospikeConverter converter =
				getMappingAerospikeConverter(new UserToAerospikeWriteDataConverter(), new AerospikeReadDataToUserConverter());

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		User user = new User(678, new Name("Nastya", "Smirnova"), null);
		converter.write(user, forWrite);

		assertThat(forWrite.getKey()).consistsOf("custom-namespace", "custom-set", 678L);
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("fs", "Nastya"), new Bin("ls", "Smirnova")
		);

		Map<String, Object> bins = of("fs", "Nastya", "ls", "Smirnova");
		User read = converter.read(User.class, AerospikeReadData.forRead(forWrite.getKey(), record(bins)));

		assertThat(read).isEqualTo(user);
	}

	@Test
	public void shouldWriteAndReadIfTypeKeyIsNull() {
		MappingAerospikeConverter converter =
				getMappingAerospikeConverter(new AerospikeTypeAliasAccessor(null));

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		User user = new User(678L, null, null);
		converter.write(user, forWrite);

		assertThat(forWrite.getKey()).consistsOf(NAMESPACE, SIMPLESET3, user.getId());
		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@user_key", "678")
		);

		Map<String, Object> bins = of("@user_key", "678");
		User read = converter.read(User.class, AerospikeReadData.forRead(forWrite.getKey(), record(bins)));

		assertThat(read).isEqualTo(user);
	}

	@Test
	public void shouldWriteExpirationValue() {
		Person person = new Person("personId", Collections.emptySet());
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();

		converter.write(person, forWrite);

		assertThat(forWrite.getExpiration()).isEqualTo(EXPIRATION_ONE_SECOND);
	}

	@Test
	public void shouldReadExpirationFieldValue() {
		Key key = new Key(NAMESPACE, "docId", 10L);

		int recordExpiration = toRecordExpiration(EXPIRATION_ONE_MINUTE);
		Record record = new Record(Collections.emptyMap(), 0, recordExpiration);

		AerospikeReadData readData = AerospikeReadData.forRead(key, record);

		DocumentWithExpirationAnnotation forRead = converter.read(DocumentWithExpirationAnnotation.class, readData);
		// Because of converting record expiration to TTL in Record.getTimeToLive method,
		// we may have expected expiration minus one second
		assertThat(forRead.getExpiration()).isIn(EXPIRATION_ONE_MINUTE, EXPIRATION_ONE_MINUTE - 1);
	}

	@Test
	public void shouldReadUnixTimeExpirationFieldValue() {
		Key key = new Key(NAMESPACE, "docId", 10L);
		int recordExpiration = toRecordExpiration(EXPIRATION_ONE_MINUTE);
		Record record = new Record(Collections.emptyMap(), 0, recordExpiration);

		AerospikeReadData readData = AerospikeReadData.forRead(key, record);
		DocumentWithUnixTimeExpiration forRead = converter.read(DocumentWithUnixTimeExpiration.class, readData);

		DateTime actual = forRead.getExpiration();
		DateTime expected = DateTime.now().plusSeconds(EXPIRATION_ONE_MINUTE);
		assertThat(actual.getMillis()).isCloseTo(expected.getMillis(), Offset.offset(100L));
	}

	@Test
	public void shouldWriteUnixTimeExpirationFieldValue() {
		DateTime unixTimeExpiration = DateTime.now().plusSeconds(EXPIRATION_ONE_MINUTE);
		DocumentWithUnixTimeExpiration document = new DocumentWithUnixTimeExpiration("docId", unixTimeExpiration);

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(document, forWrite);

		assertThat(forWrite.getExpiration()).isIn(EXPIRATION_ONE_MINUTE, EXPIRATION_ONE_MINUTE - 1);
	}

	@Test
	public void shouldFailWriteUnixTimeExpirationFieldValue() {
		DateTime unixTimeExpiration = DateTime.now().minusSeconds(EXPIRATION_ONE_MINUTE);
		DocumentWithUnixTimeExpiration document = new DocumentWithUnixTimeExpiration("docId", unixTimeExpiration);

		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();

		assertThatThrownBy(() -> converter.write(document, forWrite))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Expiration value must be greater than zero, but was: -60");
	}

	private int toRecordExpiration(int expiration) {
		ZonedDateTime documentExpiration = ZonedDateTime.now(ZoneOffset.UTC).plus(expiration, ChronoUnit.SECONDS);
		ZonedDateTime aerospikeExpirationOffset = ZonedDateTime.of(2010, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
		return (int) Duration.between(aerospikeExpirationOffset, documentExpiration).getSeconds();
	}

	@Test
	public void shouldWriteExpirationFieldValue() {
		DocumentWithExpirationAnnotation document = new DocumentWithExpirationAnnotation("docId", EXPIRATION_ONE_SECOND);
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(document, forWrite);
		assertThat(forWrite.getExpiration()).isEqualTo(EXPIRATION_ONE_SECOND);
	}

	@Test
	public void shouldNotSaveExpirationFieldAsBin() {
		DocumentWithExpirationAnnotation document = new DocumentWithExpirationAnnotation("docId", EXPIRATION_ONE_SECOND);
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(document, forWrite);
		assertThat(forWrite.getBins()).doesNotContain(new Bin("expiration", Value.get(EXPIRATION_ONE_SECOND)));
	}

	@Test
	public void shouldFailWithNullExpirationFieldValue() {
		DocumentWithExpirationAnnotation document = new DocumentWithExpirationAnnotation("docId", null);
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();

		assertThatThrownBy(() -> converter.write(document, forWrite))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Expiration must not be null!");
	}

	@Test
	public void shouldFailWithIllegalExpirationFieldValue() {
		DocumentWithExpirationAnnotation document = new DocumentWithExpirationAnnotation("docId", -1);
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();

		assertThatThrownBy(() -> converter.write(document, forWrite))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Expiration value must be greater than zero, but was: -1");
	}

	@Test
	public void shouldReadExpirationForDocumentWithDefaultConstructor() {
		int recordExpiration = toRecordExpiration(EXPIRATION_ONE_MINUTE);
		Record record = new Record(Collections.emptyMap(), 0, recordExpiration);
		Key key = new Key(NAMESPACE, "DocumentWithDefaultConstructor", "docId");
		AerospikeReadData forRead = AerospikeReadData.forRead(key, record);

		DocumentWithDefaultConstructor document = converter.read(DocumentWithDefaultConstructor.class, forRead);
		DateTime actual = document.getExpiration();
		DateTime expected = DateTime.now().plusSeconds(EXPIRATION_ONE_MINUTE);
		assertThat(actual.getMillis()).isCloseTo(expected.getMillis(), Offset.offset(100L));
	}

	@Test
	public void shouldReadExpirationForDocumentWithPersistenceConstructor() {
		int recordExpiration = toRecordExpiration(EXPIRATION_ONE_MINUTE);
		Record record = new Record(Collections.emptyMap(), 0, recordExpiration);
		Key key = new Key(NAMESPACE, "DocumentWithExpirationAnnotationAndPersistenceConstructor", "docId");
		AerospikeReadData forRead = AerospikeReadData.forRead(key, record);

		DocumentWithExpirationAnnotationAndPersistenceConstructor document = converter.read(DocumentWithExpirationAnnotationAndPersistenceConstructor.class, forRead);
		assertThat(document.getExpiration()).isCloseTo(TimeUnit.MINUTES.toSeconds(1), Offset.offset(100L));
	}

	@Test
	public void shouldNotWriteVersionToBins() {
		AerospikeWriteData forWrite = AerospikeWriteData.forWrite();
		converter.write(new VersionedClass("id", "data", 42L), forWrite);

		assertThat(forWrite.getBins()).containsOnly(
				new Bin("@user_key", "id"),
				new Bin("@_class", VersionedClass.class.getName()),
				new Bin("field", "data")
		);
	}

	@Test
	public void shouldReadObjectWithByteArrayFieldWithOneValueInData() {
		Map<String, Object> bins = new HashMap<>();
		bins.put("array", 1);
		AerospikeReadData forRead = AerospikeReadData.forRead(new Key(NAMESPACE, "DocumentWithByteArray", "user-id"), record(bins));

		DocumentWithByteArray actual = converter.read(DocumentWithByteArray.class, forRead);

		assertThat(actual).isEqualTo(new DocumentWithByteArray("user-id", new byte[]{1}));
	}

}
