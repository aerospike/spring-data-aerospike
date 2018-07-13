/**
 * 
 */
package org.springframework.data.keyvalue.repository.query;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Date;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.annotation.Id;
import org.springframework.data.keyvalue.core.query.KeyValueQuery;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.ObjectUtils;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public class SpelQueryCreatorUnitTests {
	
	static final DateTimeFormatter FORMATTER = ISODateTimeFormat.dateTimeNoMillis().withZoneUTC();

	static final Person RICKON = new Person("rickon", 4);
	static final Person BRAN = new Person("bran", 9)//
			.skinChanger(true)//
			.bornAt(FORMATTER.parseDateTime("2013-01-31T06:00:00Z").toDate());
	static final Person ARYA = new Person("arya", 13);
	static final Person ROBB = new Person("robb", 16)//
			.named("stark")//
			.bornAt(FORMATTER.parseDateTime("2010-09-20T06:00:00Z").toDate());
	static final Person JON = new Person("jon", 17).named("snow");

	@Mock 
	RepositoryMetadata metadataMock;

	static class Evaluation {
		SpelExpression expression;
		Object candidate;

		public Evaluation(SpelExpression expresison) {
			this.expression = expresison;
		}

		public Boolean against(Object candidate) {
			this.candidate = candidate;
			return evaluate();
		}

		private boolean evaluate() {
			expression.getEvaluationContext().setVariable("it", candidate);
			return expression.getValue(Boolean.class);
		}
	}

	static class Person {
		private @Id String id;
		private String firstname, lastname;
		private int age;
		private boolean isSkinChanger = false;
		private Date birthday;

		public Person() {}

		public Person(String firstname, int age) {
			super();
			this.firstname = firstname;
			this.age = age;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getFirstname() {
			return firstname;
		}

		public void setFirstname(String firstname) {
			this.firstname = firstname;
		}

		public String getLastname() {
			return lastname;
		}

		public void setLastname(String lastname) {
			this.lastname = lastname;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

		public Date getBirthday() {
			return birthday;
		}

		public void setBirthday(Date birthday) {
			this.birthday = birthday;
		}

		public boolean isSkinChanger() {
			return isSkinChanger;
		}

		public void setSkinChanger(boolean isSkinChanger) {
			this.isSkinChanger = isSkinChanger;
		}

		public Person skinChanger(boolean isSkinChanger) {
			this.isSkinChanger = isSkinChanger;
			return this;
		}

		public Person named(String lastname) {
			this.lastname = lastname;
			return this;
		}

		public Person bornAt(Date date) {
			this.birthday = date;
			return this;
		}
	}
	
	static interface PersonRepository  extends CrudRepository<Person, String> {

		// Type.SIMPLE_PROPERTY
		Person findByFirstname(String firstname);

		// Type.TRUE
		Person findBySkinChangerIsTrue();

		// Type.FALSE
		Person findBySkinChangerIsFalse();

		// Type.IS_NULL
		Person findByLastnameIsNull();

		// Type.IS_NOT_NULL
		Person findByLastnameIsNotNull();

		// Type.STARTING_WITH
		Person findByFirstnameStartingWith(String firstanme);

		Person findByFirstnameIgnoreCase(String firstanme);

		// Type.AFTER
		Person findByBirthdayAfter(Date date);

		// Type.GREATHER_THAN
		Person findByAgeGreaterThan(Integer age);

		// Type.GREATER_THAN_EQUAL
		Person findByAgeGreaterThanEqual(Integer age);

		// Type.BEFORE
		Person findByBirthdayBefore(Date date);

		// Type.LESS_THAN
		Person findByAgeLessThan(Integer age);

		// Type.LESS_THAN_EQUAL
		Person findByAgeLessThanEqual(Integer age);

		// Type.BETWEEN
		Person findByAgeBetween(Integer low, Integer high);

		// Type.LIKE
		Person findByFirstnameLike(String firstname);

		// Type.ENDING_WITH
		Person findByFirstnameEndingWith(String firstname);

		Person findByAgeGreaterThanAndLastname(Integer age, String lastname);

		Person findByAgeGreaterThanOrLastname(Integer age, String lastname);

		// Type.REGEX
		Person findByLastnameMatches(String lastname);
	}
	
	private Evaluation evaluate(String methodName, Object... args) throws Exception {
		return new Evaluation((SpelExpression) createQueryForMethodWithArgs(methodName, args).getCritieria());
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private KeyValueQuery<SpelExpression> createQueryForMethodWithArgs(String methodName, Object... args)
			throws NoSuchMethodException, SecurityException {

		Class<?>[] argTypes = new Class<?>[args.length];
		if (!ObjectUtils.isEmpty(args)) {
			for (int i = 0; i < args.length; i++) {
				argTypes[i] = args[i].getClass();
			}
		}

		Method method = PersonRepository.class.getMethod(methodName, argTypes);
		when(metadataMock.getReturnedDomainClass(method)).thenReturn((Class) Person.class);
		PartTree partTree = new PartTree(method.getName(), method.getReturnType());
		SpelQueryCreator creator = new SpelQueryCreator(partTree, new ParametersParameterAccessor(new QueryMethod(method,metadataMock, new SpelAwareProxyProjectionFactory()).getParameters(), args));

		KeyValueQuery<SpelExpression> q = creator.createQuery();
		q.getCritieria().setEvaluationContext(new StandardEvaluationContext(args));

		return q;
	}
	
	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}
	
	@Test
	public void equalsReturnsTrueWhenMatching() throws Exception {
		assertThat(evaluate("findByFirstname", RICKON.firstname).against(RICKON), is(true));
	}
}
