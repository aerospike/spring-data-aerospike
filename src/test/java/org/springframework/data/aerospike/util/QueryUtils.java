package org.springframework.data.aerospike.util;

import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.convert.AerospikeCustomConversions;
import org.springframework.data.aerospike.convert.AerospikeTypeAliasAccessor;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.repository.query.AerospikeQueryCreator;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.PersonRepository;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.data.domain.Pageable;
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
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

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

    public static Query createQueryForMethodWithArgs(ServerVersionSupport versionSupport, String methodName,
                                                     Object... args) {
        return createQueryForMethodWithArgs(PersonRepository.class, Person.class, versionSupport, methodName, args);
    }

    public static Query createQueryForMethodWithArgs(Class<?> repositoryClass, Class<?> entityClass,
                                                     ServerVersionSupport versionSupport,
                                                     String methodName, Object... args) {
        //noinspection rawtypes
        Class[] argTypes = Stream.of(args).map(Object::getClass).toArray(Class[]::new);
        //noinspection rawtypes
        Class[] argTypesCheckedForPageable = checkForPageable(argTypes);
        Method method = ReflectionUtils.findMethod(repositoryClass, methodName, argTypesCheckedForPageable);

        if (method == null) {
            //noinspection rawtypes
            Class[] argTypesToPrimitives = Stream.of(argTypesCheckedForPageable).map(c -> {
                if (ClassUtils.isPrimitiveOrWrapper(c)) {
                    return MethodType.methodType(c).unwrap().returnType();
                }
                return c;
            }).toArray(Class[]::new);
            method = ReflectionUtils.findMethod(repositoryClass, methodName, argTypesToPrimitives);
        }

        PartTree partTree = new PartTree(method.getName(), entityClass);

        AerospikeMappingContext context = new AerospikeMappingContext();
        AerospikeCustomConversions conversions = new AerospikeCustomConversions(Collections.emptyList());
        MappingAerospikeConverter converter = getMappingAerospikeConverter(conversions);

        AerospikeQueryCreator creator =
            new AerospikeQueryCreator(partTree,
                new ParametersParameterAccessor(
                    new QueryMethod(method, new DefaultRepositoryMetadata(repositoryClass),
                        new SpelAwareProxyProjectionFactory()).getParameters(), args), context, converter,
                versionSupport);
        return creator.createQuery();
    }

    /**
     * Check instances of Pageable and use the interface as we do in repositories' methods
     *
     * @param argTypes Types of arguments
     * @return Array of arguments types with Pageable instances replaced with Pageable
     */
    private static Class<?>[] checkForPageable(Class<?>[] argTypes) {
        return Arrays.stream(argTypes).map(QueryUtils::checkForPageable).toArray(Class[]::new);
    }

    private static Class<?> checkForPageable(Class<?> argType) {
        if (Pageable.class.isAssignableFrom(argType)) {
            return Pageable.class;
        }
        return argType;
    }

    private static MappingAerospikeConverter getMappingAerospikeConverter(AerospikeCustomConversions conversions) {
        MappingAerospikeConverter converter = new MappingAerospikeConverter(new AerospikeMappingContext(),
            conversions, new AerospikeTypeAliasAccessor(), new AerospikeDataSettings());
        converter.afterPropertiesSet();
        return converter;
    }
}
