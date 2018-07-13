/**
 * 
 */
package org.springframework.data.aerospike.mapping;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.context.ApplicationContext;
import org.springframework.data.aerospike.core.Person;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public class AerospikeMappingContextTest {
	
	@Mock ApplicationContext applicationContext;

	@Rule public ExpectedException exception = ExpectedException.none();

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		 MockitoAnnotations.initMocks(this.getClass()); 
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.mapping.AerospikeMappingContext#setFieldNamingStrategy(org.springframework.data.mapping.model.FieldNamingStrategy)}.
	 */
	@Test
	public void testSetFieldNamingStrategy() {
		AerospikeMappingContext context = new AerospikeMappingContext();
		context.setApplicationContext(applicationContext);
		context.setFieldNamingStrategy(null);
		
		AerospikePersistentEntity<?> entity = context.getPersistentEntity(Person.class);
		assertThat(entity.getPersistentProperty("firstName").getField().getName(), is("firstName"));
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.mapping.AerospikeMappingContext#createPersistentEntity(org.springframework.data.util.TypeInformation)}.
	 */
	@Test
	public void testCreatePersistentEntityTypeInformationOfT() {
		AerospikeMappingContext context = new AerospikeMappingContext();
		context.setApplicationContext(applicationContext);
		context.setFieldNamingStrategy(null);
		
		AerospikePersistentEntity<?> entity = context.getPersistentEntity(Person.class);
		assertThat(entity.getTypeInformation().getType().getSimpleName(), is(Person.class.getSimpleName()));
	}

}
