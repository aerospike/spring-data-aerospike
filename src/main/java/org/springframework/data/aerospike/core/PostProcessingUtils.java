/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.core;

import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.domain.Sort;
import org.springframework.lang.Nullable;
import reactor.core.publisher.Flux;

import java.util.Comparator;
import java.util.stream.Stream;

/**
 * A utility class providing methods for post-processing queries results.
 */
public final class PostProcessingUtils {

    private PostProcessingUtils() {
        throw new UnsupportedOperationException("Utility class PostProcessingUtils cannot be instantiated");
    }

    /**
     * Applies post-processing operations (sorting, offset, limit) to a given {@link Stream} based on the provided
     * {@link Query} object. If the query specifies sorting, the stream is sorted using a comparator derived from the
     * query. If an offset is present, elements are skipped. If a limit (rows) is present, the stream is truncated.
     *
     * @param <T>     The type of elements in the stream
     * @param results The input {@link Stream} of results
     * @param query   The {@link Query} object specifying criteria. Can be {@code null}
     * @return A new {@link Stream} with post-processing operations applied
     */
    public static <T> Stream<T> applyPostProcessingOnResults(Stream<T> results, @Nullable Query query) {
        if (query == null) return results;

        if (query.getSort() != null && query.getSort().isSorted()) {
            Comparator<T> comparator = TemplateUtils.getComparator(query);
            results = results.sorted(comparator);
        }
        if (query.hasOffset()) {
            results = results.skip(query.getOffset());
        }
        if (query.hasRows()) {
            results = results.limit(query.getRows());
        }
        return results;
    }

    /**
     * Applies post-processing operations (sorting, offset, limit) to a given {@link Stream} using explicit sort,
     * offset, and limit parameters. If sorting is specified, the stream is sorted using a comparator derived from the
     * {@link Sort} object. If offset is greater than 0, elements are skipped. If limit is greater than 0, the stream is
     * truncated.
     *
     * @param <T>     The type of elements in the stream
     * @param results The input {@link Stream} of results
     * @param sort    The {@link Sort} object specifying sorting criteria. Can be {@code null}
     * @param offset  The number of elements to skip from the beginning of the stream
     * @param limit   The maximum number of elements to retain in the stream
     * @return A new {@link Stream} with post-processing operations applied
     */
    static <T> Stream<T> applyPostProcessingOnResults(Stream<T> results, Sort sort, long offset, long limit) {
        if (sort != null && sort.isSorted()) {
            Comparator<T> comparator = TemplateUtils.getComparator(sort);
            results = results.sorted(comparator);
        }

        if (offset > 0) {
            results = results.skip(offset);
        }

        if (limit > 0) {
            results = results.limit(limit);
        }
        return results;
    }

    /**
     * Applies post-processing operations (sorting, offset, limit) to a given {@link Flux} of results based on the
     * provided {@link Query} object. If the query specifies sorting, the flux is sorted. If an offset is present,
     * elements are skipped. If a limit (rows) is present, the flux is truncated.
     *
     * @param <T>     The type of elements in the flux
     * @param results The input {@link Flux} of results
     * @param query   The {@link Query} object containing criteria. Can be {@code null}
     * @return A new {@link Flux} with post-processing operations applied
     */
    static <T> Flux<T> applyPostProcessingOnResults(Flux<T> results, @Nullable Query query) {
        if (query == null) return results;

        if (query.getSort() != null && query.getSort().isSorted()) {
            Comparator<T> comparator = TemplateUtils.getComparator(query);
            results = results.sort(comparator);
        }

        if (query.hasOffset()) {
            results = results.skip(query.getOffset());
        }
        if (query.hasRows()) {
            results = results.take(query.getRows());
        }
        return results;
    }

    /**
     * Applies post-processing operations (sorting, offset, limit) to a given {@link Flux} of results using explicit
     * sort, offset, and limit parameters. If sorting is specified, the flux is sorted. If offset is greater than 0,
     * elements are skipped. If limit is greater than 0, the flux is truncated.
     *
     * @param <T>     The type of elements in the flux
     * @param results The input {@link Flux} of results
     * @param sort    The {@link Sort} object specifying sorting criteria. Can be {@code null}
     * @param offset  The number of elements to skip from the beginning of the flux
     * @param limit   The maximum number of elements to retain in the flux
     * @return A new {@link Flux} with post-processing operations applied
     */
    static <T> Flux<T> applyPostProcessingOnResults(Flux<T> results, Sort sort, long offset, long limit) {
        if (sort != null && sort.isSorted()) {
            Comparator<T> comparator = TemplateUtils.getComparator(sort);
            results = results.sort(comparator);
        }

        if (offset > 0) {
            results = results.skip(offset);
        }

        if (limit > 0) {
            results = results.take(limit);
        }
        return results;
    }
}
