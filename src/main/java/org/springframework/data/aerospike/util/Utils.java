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
package org.springframework.data.aerospike.util;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.exp.Exp;
import lombok.experimental.UtilityClass;
import org.springframework.dao.InvalidDataAccessResourceUsageException;

import java.io.File;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.temporal.Temporal;
import java.util.Currency;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

import static com.aerospike.client.command.ParticleType.BOOL;
import static com.aerospike.client.command.ParticleType.INTEGER;
import static com.aerospike.client.command.ParticleType.LIST;
import static com.aerospike.client.command.ParticleType.MAP;
import static com.aerospike.client.command.ParticleType.STRING;
import static org.springframework.util.ClassUtils.isPrimitiveOrWrapper;

/**
 * Utility class containing useful methods for interacting with Aerospike across the entire implementation
 *
 * @author peter
 */
@UtilityClass
public class Utils {

    /**
     * Issues an "Info" request to all nodes in the cluster.
     *
     * @param client     An IAerospikeClient.
     * @param infoString The name of the variable to retrieve.
     * @return An "Info" value for the given variable from all the nodes in the cluster.
     */
    @SuppressWarnings("UnusedReturnValue")
    public static String[] infoAll(IAerospikeClient client,
                                   String infoString) {
        String[] messages = new String[client.getNodes().length];
        int index = 0;
        for (Node node : client.getNodes()) {
            messages[index] = Info.request(client.getInfoPolicyDefault(), node, infoString);
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
        if (nodes.length == 0) {
            throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Command failed because cluster is empty.");
        }
        int offset = ThreadLocalRandom.current().nextInt(nodes.length);
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
        if (infoString.isEmpty()) { // set is not present
            return 0L;
        }
        return InfoResponseUtils.getPropertyFromInfoResponse(infoString, "objects", Long::parseLong);
    }

    public static Exp getValueExpOrFail(Value value, String errMsg) {
        return switch (value.getType()) {
            case INTEGER -> Exp.val(value.toLong());
            case STRING -> Exp.val(value.toString());
            case BOOL -> Exp.val((Boolean) value.getObject());
            case LIST -> Exp.val((List<?>) value.getObject());
            case MAP -> Exp.val((Map<?, ?>) value.getObject());
            case ParticleType.NULL -> Exp.nil();
            default -> throw new UnsupportedOperationException(errMsg);
        };
    }

    /**
     * Checking that at least one of the arguments is of the following type: a primitive or primitive wrapper, an Enum,
     * a String or other CharSequence, a Number, a Date, a Temporal, a UUID, a URI, a URL, a Locale, or a Class.
     */
    public static boolean isSimpleValueType(Class<?> type) {
        return (Void.class != type && void.class != type &&
            (isPrimitiveOrWrapper(type) ||
                Enum.class.isAssignableFrom(type) ||
                CharSequence.class.isAssignableFrom(type) ||
                Number.class.isAssignableFrom(type) ||
                Date.class.isAssignableFrom(type) ||
                Temporal.class.isAssignableFrom(type) ||
                ZoneId.class.isAssignableFrom(type) ||
                TimeZone.class.isAssignableFrom(type) ||
                File.class.isAssignableFrom(type) ||
                Path.class.isAssignableFrom(type) ||
                Charset.class.isAssignableFrom(type) ||
                Currency.class.isAssignableFrom(type) ||
                InetAddress.class.isAssignableFrom(type) ||
                URI.class == type ||
                URL.class == type ||
                UUID.class == type ||
                Locale.class == type ||
                Pattern.class == type ||
                Class.class == type));
    }
}
