/*
 * Copyright 2019 the original author or authors
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
package org.springframework.data.aerospike.mapping;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.data.aerospike.sample.Person;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public class CachingAerospikePersistentPropertyTest {

	AerospikeMappingContext context;

	@BeforeEach
	public void setUp() {
		context = new AerospikeMappingContext();
		context.setApplicationContext(mock(ApplicationContext.class));
	}

	@Test
	public void isTransient() {
		AerospikePersistentEntity<?> entity = context.getPersistentEntity(Person.class);

		assertThat(entity.getIdProperty().isTransient()).isFalse();
	}

	@Test
	public void isAssociation() {
		AerospikePersistentEntity<?> entity = context.getPersistentEntity(Person.class);

		assertThat(entity.getIdProperty().isAssociation()).isFalse();
	}

	@Test
	public void usePropertyAccess() {
		AerospikePersistentEntity<?> entity = context.getPersistentEntity(Person.class);

		assertThat(entity.getIdProperty().usePropertyAccess()).isFalse();
	}

	@Test
	public void isIdProperty() {
		AerospikePersistentEntity<?> entity = context.getPersistentEntity(Person.class);

		assertThat(entity.getIdProperty().isIdProperty()).isTrue();
	}

	@Test
	public void getFieldName() {
		AerospikePersistentEntity<?> entity = context.getPersistentEntity(Person.class);

		assertThat(entity.getIdProperty().getName()).isEqualTo("id");
	}
}
