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

import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.data.aerospike.sample.Person;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public class AerospikeMappingContextTest {
	
	@Test
	public void setFieldNamingStrategy() {
		AerospikeMappingContext context = new AerospikeMappingContext();
		context.setApplicationContext(mock(ApplicationContext.class));
		context.setFieldNamingStrategy(null);
		
		AerospikePersistentEntity<?> entity = context.getPersistentEntity(Person.class);

		assertThat(entity.getPersistentProperty("firstName").getField().getName()).isEqualTo("firstName");
	}

	@Test
	public void createPersistentEntityTypeInformationOfT() {
		AerospikeMappingContext context = new AerospikeMappingContext();
		context.setApplicationContext(mock(ApplicationContext.class));
		context.setFieldNamingStrategy(null);
		
		AerospikePersistentEntity<?> entity = context.getPersistentEntity(Person.class);

		assertThat(entity.getTypeInformation().getType().getSimpleName()).isEqualTo(Person.class.getSimpleName());
	}
}
