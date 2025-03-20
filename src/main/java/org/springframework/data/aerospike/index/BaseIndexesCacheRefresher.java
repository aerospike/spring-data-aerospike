package org.springframework.data.aerospike.index;

public interface BaseIndexesCacheRefresher<T> {

    T refreshIndexesCache();
}
