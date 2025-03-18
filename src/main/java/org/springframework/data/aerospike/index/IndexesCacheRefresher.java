package org.springframework.data.aerospike.index;

public interface IndexesCacheRefresher extends BaseIndexesCacheRefresher {

    void refreshIndexesCache();
}
