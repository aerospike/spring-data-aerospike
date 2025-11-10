package org.springframework.data.aerospike.repository.query;

import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.aerospike.annotation.Query;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.lang.Nullable;
import org.springframework.util.ConcurrentReferenceHashMap;
import org.springframework.util.StringUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;

public class AerospikeQueryMethod extends QueryMethod {

    private final Map<Class<? extends Annotation>, Optional<Annotation>> annotationCache;
    private final Method method;

    /**
     * Creates a new {@link QueryMethod} from the given parameters. Looks up the correct query to use for following
     * invocations of the method given.
     *
     * @param method   must not be {@literal null}.
     * @param metadata must not be {@literal null}.
     * @param factory  must not be {@literal null}.
     */
    public AerospikeQueryMethod(Method method, RepositoryMetadata metadata, ProjectionFactory factory) {
        super(method, metadata, factory, null);
        this.method = method;
        this.annotationCache = new ConcurrentReferenceHashMap<>();
    }

    @SuppressWarnings("unchecked")
    private <A extends Annotation> Optional<A> lookupInAnnotationCache(Class<A> annotationType) {
        return (Optional<A>) this.annotationCache.computeIfAbsent(annotationType,
            it -> Optional.ofNullable(AnnotatedElementUtils.findMergedAnnotation(method, it)));
    }

    /**
     * Returns whether the method has an annotated query.
     */
    public boolean hasQueryAnnotation() {
        return findQueryAnnotation().isPresent();
    }

    /**
     * Returns the DSL expression string declared in the {@link org.springframework.data.aerospike.annotation.Query}
     * annotation or {@literal null}.
     */
    @Nullable
    String getQueryAnnotation() {
        return findQueryAnnotation().orElse(null);
    }

    private Optional<String> findQueryAnnotation() {
        return lookupQueryAnnotation() //
            .map(Query::expression) //
            .filter(StringUtils::hasText);
    }

    Optional<Query> lookupQueryAnnotation() {
        return lookupInAnnotationCache(Query.class);
    }
}
