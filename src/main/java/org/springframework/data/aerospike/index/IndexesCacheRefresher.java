package org.springframework.data.aerospike.index;

public interface IndexesCacheRefresher<T> {

    T refreshIndexesCache();
}
