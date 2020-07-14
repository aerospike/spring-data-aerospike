package org.springframework.data.aerospike.query.cache;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.InfoPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.aerospike.query.model.IndexesInfo;

import java.util.Arrays;

/**
 * @author Anastasiia Smirnova
 */
public class IndexRefresher {

	private final Logger log = LoggerFactory.getLogger(IndexesCacheHolder.class);

	private final IAerospikeClient client;
	private final InfoPolicy infoPolicy;
	private final InternalIndexOperations indexOperations;
	private final IndexesCacheUpdater indexesCacheUpdater;

	public IndexRefresher(IAerospikeClient client, InfoPolicy infoPolicy,
						  InternalIndexOperations indexOperations, IndexesCacheUpdater indexesCacheUpdater) {
		this.client = client;
		this.infoPolicy = infoPolicy;
		this.indexOperations = indexOperations;
		this.indexesCacheUpdater = indexesCacheUpdater;
	}

	public void refreshIndexes() {
		log.trace("Loading indexes");
		IndexesInfo cache = Arrays.stream(client.getNodes())
				.filter(Node::isActive)
				.findAny() // we do want to send info request to the random node (sending request to the first node may lead to uneven request distribution)
				.map(node -> Info.request(infoPolicy, node, indexOperations.buildGetIndexesCommand()))
				.map(response -> indexOperations.parseIndexesInfo(response))
				.orElse(IndexesInfo.empty());
		log.debug("Loaded indexes: {}", cache.indexes);
		this.indexesCacheUpdater.update(cache);
	}
}
