package org.springframework.data.aerospike.config;

import java.lang.reflect.Method;

public class IndexedBinsAnnotationsProcessor {

    public static boolean hasAssertBinsAreIndexedAnnotation(Method testMethod) {
        AssertBinsAreIndexed annotation = testMethod.getAnnotation(AssertBinsAreIndexed.class);
        return annotation != null;
    }

    public static boolean hasNoindexAnnotation(Method testMethod) {
        NoSecondaryIndexRequired annotation = testMethod.getAnnotation(NoSecondaryIndexRequired.class);
        return annotation != null;
    }

    public static String[] getBinNames(Method testMethod) {
        AssertBinsAreIndexed annotation = testMethod.getAnnotation(AssertBinsAreIndexed.class);
        if (annotation != null) {
            return annotation.binNames();
        }
        return null;
    }

    public static Class<?> getEntityClass(Method testMethod) {
        AssertBinsAreIndexed annotation = testMethod.getAnnotation(AssertBinsAreIndexed.class);
        if (annotation != null) {
            return annotation.entityClass();
        }
        return null;
    }
}
