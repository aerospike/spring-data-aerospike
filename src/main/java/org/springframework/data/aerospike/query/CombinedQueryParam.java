package org.springframework.data.aerospike.query;

import lombok.Getter;

/**
 * This class stores arguments passed to each part of a combined repository query,
 * e.g., repository.findByNameAndEmail(CombinedQueryParam.of("John"), CombinedQueryParam.of("john@email.com"))
 */
public record CombinedQueryParam(@Getter Object[] arguments) {

    /**
     * This method is required to pass arguments to each part of a combined query
     * @param arguments necessary objects
     * @return instance of {@link CombinedQueryParam}
     */
    public static CombinedQueryParam of(Object... arguments) {
        return new CombinedQueryParam(arguments);
    }
}
