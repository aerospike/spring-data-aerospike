/**
 * 
 */
package org.springframework.data.aerospike.mapping;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.springframework.context.ApplicationContext;
import org.springframework.data.aerospike.core.Person;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public class CachingAerospikePersistentPropertyTest {

	@Mock ApplicationContext applicationContext;

	@Rule public ExpectedException exception = ExpectedException.none();

	AerospikeMappingContext context = null;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		context = new AerospikeMappingContext();
		context.setApplicationContext(applicationContext);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.mapping.CachingAerospikePersistentProperty#isTransient()}.
	 */
	@Test
	public void testIsTransient() {
		AerospikePersistentEntity<?> entity = context.getPersistentEntity(Person.class);
		assertFalse(entity.getIdProperty().isTransient());
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.mapping.CachingAerospikePersistentProperty#isAssociation()}.
	 */
	@Test
	public void testIsAssociation() {
		AerospikePersistentEntity<?> entity = context.getPersistentEntity(Person.class);
		assertFalse(entity.getIdProperty().isAssociation());
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.mapping.CachingAerospikePersistentProperty#usePropertyAccess()}.
	 */
	@Test
	public void testUsePropertyAccess() {
		AerospikePersistentEntity<?> entity = context.getPersistentEntity(Person.class);
		assertFalse(entity.getIdProperty().usePropertyAccess());
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.mapping.CachingAerospikePersistentProperty#isIdProperty()}.
	 */
	@Test
	public void testIsIdProperty() {
		AerospikePersistentEntity<?> entity = context.getPersistentEntity(Person.class);
		assertTrue(entity.getIdProperty().isIdProperty());
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.mapping.CachingAerospikePersistentProperty#getFieldName()}.
	 */
	@Test
	public void testGetFieldName() {
		AerospikePersistentEntity<?> entity = context.getPersistentEntity(Person.class);
		assertEquals("id", entity.getIdProperty().getName());
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.mapping.CachingAerospikePersistentProperty#CachingAerospikePersistentProperty(java.lang.reflect.Field, java.beans.PropertyDescriptor, org.springframework.data.mapping.PersistentEntity, org.springframework.data.mapping.model.SimpleTypeHolder, org.springframework.data.mapping.model.FieldNamingStrategy)}.
	 */

}
