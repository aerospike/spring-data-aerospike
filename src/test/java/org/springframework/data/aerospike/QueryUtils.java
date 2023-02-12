package org.springframework.data.aerospike;

import org.springframework.data.aerospike.repository.query.AerospikeQueryCreator;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.PersonRepository;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.testcontainers.shaded.org.apache.commons.lang3.ClassUtils.wrapperToPrimitive;
import static org.testcontainers.shaded.org.apache.commons.lang3.ClassUtils.wrappersToPrimitives;

public class QueryUtils {

	private static final Map<Class<?>, Class<?>> WRAPPERS_TO_PRIMITIVES
			= new ImmutableMap.Builder<Class<?>, Class<?>>()
			.put(Boolean.class, boolean.class)
			.put(Byte.class, byte.class)
			.put(Character.class, char.class)
			.put(Double.class, double.class)
			.put(Float.class, float.class)
			.put(Integer.class, int.class)
			.put(Long.class, long.class)
			.put(Short.class, short.class)
			.put(Void.class, void.class)
			.build();

	@SuppressWarnings("unchecked")
	private static <T> Class<T> unwrap(Class<T> c) {
		return c.isPrimitive() ? (Class<T>) WRAPPERS_TO_PRIMITIVES.get(c) : c;
	}

	public static <T> Query createQueryForMethodWithArgs(String methodName, Object... args) {
		Class[] argTypes = Stream.of(args).map(Object::getClass).toArray(Class[]::new);
		Method method = ReflectionUtils.findMethod(PersonRepository.class, methodName, argTypes);

		if (method == null) {
			Class[] argTypesToPrimitives = Stream.of(args).map(Object::getClass).map(c -> {
				if (ClassUtils.isPrimitiveOrWrapper(c)) {
					return MethodType.methodType(c).unwrap().returnType();
				}
				return c;
			}).toArray(Class[]::new);
			method = ReflectionUtils.findMethod(PersonRepository.class, methodName, argTypesToPrimitives);
		}

		PartTree partTree = new PartTree(method.getName(), Person.class);
		AerospikeQueryCreator creator =
				new AerospikeQueryCreator(partTree,
						new ParametersParameterAccessor(
								new QueryMethod(method, new DefaultRepositoryMetadata(PersonRepository.class),
										new SpelAwareProxyProjectionFactory()).getParameters(), args));
		return creator.createQuery();
	}
}
