package org.springframework.data.aerospike.util;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.listener.InfoListener;
import com.aerospike.client.policy.InfoPolicy;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
public class InfoCommandUtils {

    public static String sendInfoCommand(IAerospikeClient client, Node node, String command) {
        return sendInfoCommand(client, client.getInfoPolicyDefault(), node, command);
    }

    public static String sendInfoCommand(IAerospikeClient client, InfoPolicy infoPolicy, Node node, String command) {
        InfoListenerWithStringValue listener = new InfoListenerWithStringValue() {

            volatile String stringValue = "";
            volatile String infoCommand = "";
            volatile boolean isComplete = false;
            volatile AerospikeException exception;

            @Override
            public synchronized String getStringValue() {
                return stringValue;
            }

            @Override
            public synchronized boolean isComplete() {
                return isComplete;
            }

            @Override
            public synchronized AerospikeException getException() {
                return exception;
            }

            @Override
            public synchronized String getInfoCommand() {
                return infoCommand;
            }

            @Override
            public void onSuccess(Map<String, String> map) {
                stringValue = map.get(command);
                isComplete = true;
            }

            @Override
            public void onFailure(AerospikeException ae) {
                exception = ae;
                infoCommand = command;
                isComplete = true;
            }
        };

        client.info(client.getCluster().eventLoops.next(), listener, infoPolicy, node, command);
        waitForCompletionOrTimeout(listener);
        failIfExceptionFound(listener);

        return listener.getStringValue() == null ? "" : listener.getStringValue();
    }

    private static void failIfExceptionFound(InfoListenerWithStringValue listener) {
        if (listener.getException() != null) {
            throw new AerospikeException(String.format("Info command %s failed", listener.getInfoCommand()),
                listener.getException());
        }
    }

    private static void waitForCompletionOrTimeout(InfoListenerWithStringValue listener) {
        // Create a CountDownLatch with initial count 1
        CountDownLatch latch = new CountDownLatch(1);

        // Start a separate thread to wait for isComplete()
        Thread waitingThread = getWaitingThread(listener, latch);

        try {
            // Wait for completion or timeout
            boolean timeoutOver = latch.await(1, TimeUnit.SECONDS); // timeout is 1 second
            if (!timeoutOver) {
                waitingThread.interrupt(); // Interrupt waiting thread if timeout occurs
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Interrupted
            log.error("Interrupted while waiting for info command to complete");
        }
    }

    private static Thread getWaitingThread(InfoListenerWithStringValue listener, CountDownLatch latch) {
        Thread waitingThread = new Thread(() -> {
            while (!listener.isComplete()) {
                try {
                    //noinspection ResultOfMethodCallIgnored
                    latch.await(1, TimeUnit.MILLISECONDS); // Wait briefly before re-checking
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return; // Interrupted, exit thread
                }
            }
            latch.countDown(); // Release latch when isComplete() is true
        });
        waitingThread.start();
        return waitingThread;
    }

    interface InfoListenerWithStringValue extends InfoListener {

        String getStringValue();

        @SuppressWarnings("BooleanMethodIsAlwaysInverted")
        boolean isComplete();

        AerospikeException getException();

        String getInfoCommand();
    }
}
