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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import lombok.experimental.UtilityClass;

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
	 * @param client An AerospikeClient.
	 * @param infoString The name of the variable to retrieve.
	 * @return An "Info" value for the given variable from all the nodes in the cluster.
	 */
	public static String[] infoAll(AerospikeClient client, String infoString) {
		String[] messages = new String[client.getNodes().length];
		int index = 0;
		for (Node node : client.getNodes()) {
			messages[index] = Info.request(node, infoString);
		}
		return messages;
	}
}