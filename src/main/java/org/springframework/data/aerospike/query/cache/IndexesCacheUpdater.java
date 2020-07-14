package org.springframework.data.aerospike.query.cache;

import org.springframework.data.aerospike.query.model.IndexesInfo;

public interface IndexesCacheUpdater {

	void update(IndexesInfo cache);
}
