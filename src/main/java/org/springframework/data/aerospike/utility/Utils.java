/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.utility;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Node;
import lombok.experimental.UtilityClass;
import org.springframework.dao.InvalidDataAccessResourceUsageException;

import java.util.Random;

/**
 * Utility class containing useful methods
 * for interacting with Aerospike
 * across the entire implementation
 * @author peter
 */
@UtilityClass
public class Utils {
	/**
	 * Issues an "Info" request to all nodes in the cluster.
	 * @param client An IAerospikeClient.
	 * @param infoString The name of the variable to retrieve.
	 * @return An "Info" value for the given variable from all the nodes in the cluster.
	 */
	public static String[] infoAll(IAerospikeClient client,
								   String infoString) {
		String[] messages = new String[client.getNodes().length];
		int index = 0;
		for (Node node : client.getNodes()) {
			messages[index] = Info.request(node, infoString);
		}
		return messages;
	}

	public static int getReplicationFactor(Node[] nodes, String namespace) {
		Node randomNode = getRandomNode(nodes);

		String response = Info.request(randomNode, "get-config:context=namespace;id=" + namespace);
		if (response.equalsIgnoreCase("ns_type=unknown")) {
			throw new InvalidDataAccessResourceUsageException("Namespace: " + namespace + " does not exist");
		}
		return InfoResponseUtils.getPropertyFromConfigResponse(response, "replication-factor", Integer::parseInt);
	}

	public static Node getRandomNode(Node[] nodes) {
		Random random = new Random();

		if (nodes.length == 0) {
			throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Command failed because cluster is empty.");
		}
		int offset = random.nextInt(nodes.length);
		for (int i = 0; i < nodes.length; i++) {
			int index = (offset + i) % nodes.length;
			Node node = nodes[index];
			if (node.isActive()) {
				return node;
			}
		}
		throw new AerospikeException.InvalidNode("Command failed because no active nodes found.");
	}

	public static long getObjectsCount(Node node, String namespace, String setName) {
		String infoString = Info.request(node, "sets/" + namespace + "/" + setName);
		if (infoString.isEmpty()) {// set is not present
			return 0L;
		}
		return InfoResponseUtils.getPropertyFromInfoResponse(infoString, "objects", Long::parseLong);
	}
}