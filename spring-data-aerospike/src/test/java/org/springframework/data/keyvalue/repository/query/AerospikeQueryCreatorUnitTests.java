/**
 *
 */
package org.springframework.data.keyvalue.repository.query;


import java.lang.reflect.Method;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.aerospike.convert.AerospikeConverter;
import org.springframework.data.aerospike.core.Person;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.repository.query.AerospikeQueryCreator;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.repository.query.StubParameterAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.repository.query.parser.PartTree;


/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public class AerospikeQueryCreatorUnitTests {

	MappingContext<?, AerospikePersistentProperty> context;
	Method findByFirstname, findByFirstnameAndFriend, findByFirstnameNotNull, findByFirstNameIn;
	@Mock
	AerospikeConverter converter;

	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
		context = new AerospikeMappingContext();
	}

	@SuppressWarnings("unused")
	@Test
	public void createsQueryCorrectly() throws Exception {
		PartTree tree = new PartTree("findByFirstName", Person.class);

		AerospikeQueryCreator creator = new AerospikeQueryCreator(tree, new StubParameterAccessor(converter, "Oliver"), context);
		Query query = creator.createQuery();
	}
	
	@SuppressWarnings("unused")
	@Test
	public void createQueryByInList(){
		PartTree tree = new PartTree("findByFirstNameOrFriend", Person.class);

		AerospikeQueryCreator creator = new AerospikeQueryCreator(tree, new StubParameterAccessor(converter, (Object[])new String[]{"Oliver", "Peter"}), context);
		Query query = creator.createQuery();	
	}

}
