package org.springframework.data.aerospike.core;

import org.springframework.data.aerospike.query.Qualifier;

import java.util.Collection;
import java.util.List;

public interface AerospikeInternalOperations {

    /**
     * Find document by providing id, set name will be determined by the given entityClass.
     * <p>
     * Documents will be mapped to the given targetClass.
     *
     * @param id          The id of the document to find. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param targetClass The class to map the document to.
     * @param qualifiers  {@link Qualifier}s provided to build a filter Expression for the query. Optional argument.
     * @return The document from Aerospike, returned document will be mapped to targetClass's type.
     */
    <T, S> Object findByIdInternal(Object id, Class<T> entityClass, Class<S> targetClass, Qualifier... qualifiers);

    /**
     * Find documents by providing multiple ids, set name will be determined by the given entityClass.
     * <p>
     * Documents will be mapped to the given targetClass.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param targetClass The class to map the document to.
     * @param qualifiers  {@link Qualifier}s provided to build a filter Expression for the query. Optional argument.
     * @return The documents from Aerospike, returned documents will be mapped to targetClass's type, if no document
     * exists, an empty list is returned.
     */
    <T, S> List<?> findByIdsInternal(Collection<?> ids, Class<T> entityClass, Class<S> targetClass,
                                     Qualifier... qualifiers);

    /**
     * Delete document by providing id, set name will be determined by the given entityClass.
     * <p>
     *
     * @param ids         The ids of the documents to delete. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     */
    <T> void deleteByIdsInternal(Collection<?> ids, Class<T> entityClass);

}
