package org.springframework.data.aerospike.util;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.listener.InfoListener;
import com.aerospike.client.policy.InfoPolicy;
import lombok.experimental.UtilityClass;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

@UtilityClass
public class InfoCommandUtils {

    public String request(IAerospikeClient client, Node node, String command) {
        return request(client, client.getInfoPolicyDefault(), node, command);
    }

    public String request(IAerospikeClient client, InfoPolicy infoPolicy, Node node, String command) {
        InfoListenerWithStringValue listener = new InfoListenerWithStringValue() {

            private final CompletableFuture<String> stringValueFuture = new CompletableFuture<>();

            public CompletableFuture<String> getValueFuture() {
                return stringValueFuture;
            }

            @Override
            public void onSuccess(Map<String, String> map) {
                try {
                    stringValueFuture.complete(map.get(command));
                } catch (Exception e) {
                    stringValueFuture.completeExceptionally(commandFailed(command, e));
                }
            }

            @Override
            public void onFailure(AerospikeException ae) {
                stringValueFuture.completeExceptionally(ae);
            }
        };

        try {
            client.info(client.getCluster().eventLoops.next(), listener, infoPolicy, node, command);
        } catch (AerospikeException ae) {
            throw commandFailed(command, ae);
        }

        String value;
        try {
            value = listener.getValueFuture().orTimeout(infoPolicy.timeout, TimeUnit.MILLISECONDS).join();
        } catch (CompletionException ce) {
            throw commandFailed(command, ce.getCause());
        }
        return value == null ? "" : value;
    }

    private AerospikeException commandFailed(String command, Throwable t) {
        return new AerospikeException(String.format("Info command %s failed", command), t);
    }

    interface InfoListenerWithStringValue extends InfoListener {

        CompletableFuture<String> getValueFuture();
    }
}
