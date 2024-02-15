package org.springframework.data.aerospike.query;

/**
 * This class stores arguments passed to each part of a combined repository query, e.g.,
 * <pre>repository.findByNameAndEmail(QueryParam.of("John"), QueryParam.of("john@email.com"))</pre>
 */
public record QueryParam(Object[] arguments) {

    /**
     * This method is required to pass arguments to each part of a combined query
     *
     * @param arguments necessary objects
     * @return instance of {@link QueryParam}
     */
    public static QueryParam of(Object... arguments) {
        return new QueryParam(arguments);
    }
}
