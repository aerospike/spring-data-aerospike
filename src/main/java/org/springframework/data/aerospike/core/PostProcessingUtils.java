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

import com.aerospike.client.policy.Policy;
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

    static <T> Stream<T> applyPostProcessingOnResults(Stream<T> results, Query query) {
        if (query != null) {
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
        }
        return results;
    }

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
