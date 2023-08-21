package org.springframework.data.aerospike.core;

import org.springframework.data.aerospike.query.Qualifier;

import java.util.Collection;
import java.util.List;

public interface AerospikeInternalOperations {

    /**
     *
     * @param id
     * @param entityClass
     * @param targetClass
     * @param qualifiers
     * @return
     * @param <T>
     * @param <S>
     */
    <T, S> Object findByIdInternal(Object id, Class<T> entityClass, Class<S> targetClass,
                                                 Qualifier... qualifiers);

    /**
     *
     * @param ids
     * @param entityClass
     * @param targetClass
     * @param qualifiers
     * @return
     * @param <T>
     * @param <S>
     */
    <T, S> List<?> findByIdsInternal(Collection<?> ids, Class<T> entityClass, Class<S> targetClass,
                                                   Qualifier... qualifiers);
}
