package org.springframework.data.aerospike.util;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.listener.InfoListener;
import com.aerospike.client.policy.InfoPolicy;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

@Slf4j
public class InfoCommandUtils {

    public static String sendInfoCommand(IAerospikeClient client, Node node, String command) {
        return sendInfoCommand(client, client.getInfoPolicyDefault(), node, command);
    }

    public static String sendInfoCommand(IAerospikeClient client, InfoPolicy infoPolicy, Node node, String command) {
        InfoListenerWithStringValue listener = new InfoListenerWithStringValue() {

            private final CompletableFuture<String> stringValueFuture = new CompletableFuture<>();

            public CompletableFuture<String> getValueFuture() {
                return stringValueFuture;
            }

            @Override
            public void onSuccess(Map<String, String> map) {
                stringValueFuture.complete(map.get(command));
            }

            @Override
            public void onFailure(AerospikeException ae) {
                stringValueFuture.completeExceptionally(ae);
            }
        };

        client.info(client.getCluster().eventLoops.next(), listener, infoPolicy, node, command);

        String value;
        try {
            value = listener.getValueFuture().join();
        } catch (CompletionException ce) {
            throw new AerospikeException(String.format("Info command %s failed", command), ce.getCause());
        }
        return value == null ? "" : value;
    }

    interface InfoListenerWithStringValue extends InfoListener {

        CompletableFuture<String> getValueFuture();
    }
}
