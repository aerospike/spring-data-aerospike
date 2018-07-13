package org.springframework.data.aerospike.utility;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;

/**
 * Utility class containing useful methods
 * for interacting with Aerospike
 * across the entire implementation
 * @author peter
 *
 */
public class Utils {
	private Utils(){
		super();
	}
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
	/**
	 * Computes the number of records in a specific namespace.set
	 * @param client
	 * @param namespace
	 * @param set
	 * @return
	 */
	public static long sizeofSet(AerospikeClient client, String namespace, String set){
		// ns_name=test:set_name=tweets:n_objects=68763:set-stop-write-count=0:set-evict-hwm-count=0:set-enable-xdr=use-default:set-delete=false;
		Pattern pattern = Pattern.compile("ns_name=" + namespace + ":set_name=" + set + ":n_objects=(\\d+)");
		String[] results = infoAll(client, "sets");
		long size = 0;
		for (String info : results){
			Matcher matcher = pattern.matcher(info);
			while (matcher.find()){
				size += Long.parseLong(matcher.group(1));
			}
		}
		return size;
	}
	
}