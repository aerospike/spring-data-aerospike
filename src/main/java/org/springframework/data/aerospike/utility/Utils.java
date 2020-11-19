package org.springframework.data.aerospike.utility;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import lombok.experimental.UtilityClass;

/**
 * Utility class containing useful methods
 * for interacting with Aerospike
 * across the entire implementation
 * @author peter
 *
 */
@UtilityClass
public class Utils {
	/**
	 * Issues an "Info" request to all nodes in the cluster.
	 * @param client
	 * @param infoString
	 * @return
	 */
	public static String[] infoAll(AerospikeClient client,
			String infoString) {
		String[] messages = new String[client.getNodes().length];
		int index = 0;
		for (Node node : client.getNodes()){
			messages[index] = Info.request(node, infoString);
		}
		return messages;
	}

}