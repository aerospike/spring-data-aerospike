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
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.exp.Exp;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import org.slf4j.Logger;
import org.springframework.core.env.Environment;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition;
import org.springframework.util.StringUtils;

import java.io.File;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Currency;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.aerospike.client.command.ParticleType.BOOL;
import static com.aerospike.client.command.ParticleType.DOUBLE;
import static com.aerospike.client.command.ParticleType.INTEGER;
import static com.aerospike.client.command.ParticleType.LIST;
import static com.aerospike.client.command.ParticleType.MAP;
import static com.aerospike.client.command.ParticleType.STRING;
import static org.springframework.util.ClassUtils.isPrimitiveOrWrapper;
import static org.springframework.util.StringUtils.hasLength;

/**
 * Utility class containing useful methods for interacting with Aerospike across the entire implementation
 *
 * @author peter
 */
@UtilityClass
public class Utils {

    public static int getReplicationFactor(IAerospikeClient client, Node[] nodes, String namespace) {
        Node randomNode = getRandomNode(nodes);
        String response = InfoCommandUtils.request(client, randomNode, "get-config:context=namespace;id=" + namespace);

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

    public static long getObjectsCount(IAerospikeClient client, Node node, String namespace, String setName) {
        String infoString = InfoCommandUtils.request(client, node, "sets/" + namespace + "/" + setName);
        if (infoString.isEmpty()) { // set is not present
            return 0L;
        }
        return InfoResponseUtils.getPropertyFromInfoResponse(infoString, "objects", Long::parseLong);
    }

    /**
     * Convert {@link Value} type to {@link Exp}
     *
     * @param value  Value instance
     * @param errMsg Error message to use
     * @return Exp instance
     */
    public static Exp getExpValOrFail(Value value, String errMsg) {
        return switch (value.getType()) {
            case INTEGER -> Exp.val(value.toLong());
            case BOOL -> Exp.val((Boolean) value.getObject());
            case STRING -> Exp.val(value.toString());
            case LIST -> Exp.val((List<?>) value.getObject());
            case MAP -> Exp.val((Map<?, ?>) value.getObject());
            case ParticleType.NULL -> Exp.nil();
            default -> throw new UnsupportedOperationException(errMsg);
        };
    }

    public static Exp.Type getExpType(Value value) {
        return switch (value.getType()) {
            case INTEGER -> Exp.Type.INT;
            case DOUBLE -> Exp.Type.FLOAT;
            case BOOL -> Exp.Type.BOOL;
            case STRING -> Exp.Type.STRING;
            case LIST -> Exp.Type.LIST;
            case MAP -> Exp.Type.MAP;
            case ParticleType.NULL -> Exp.Type.NIL;
            default -> throw new UnsupportedOperationException("Unsupported Value type: " + value.getType());
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

    public static String ctxArrToString(CTX[] ctxArr) {
        return Arrays.stream(ctxArr).map(ctx -> ctx.value.toString()).collect(Collectors.joining("."));
    }

    public static void logQualifierDetails(CriteriaDefinition criteria, Logger logger) {
        if (criteria == null || !logger.isDebugEnabled()) return;
        Qualifier qualifier = criteria.getCriteriaObject();
        Qualifier[] qualifiers = qualifier.getQualifiers();
        if (qualifiers != null && qualifiers.length > 0) {
            Arrays.stream(qualifiers).forEach(innerQualifier -> logQualifierDetails(innerQualifier, logger));
        }

        FilterOperation operation = qualifier.getOperation();
        String strOperation = (operation != null && hasLength(operation.toString()) ? operation.toString() : "N/A");

        String values = "";
        String value = valueToString(qualifier.getValue());
        String value2 = valueToString(qualifier.getSecondValue());
        values = hasLength(value) ? String.format(", value = %s", value) : "";
        values = hasLength(value2) ? String.format("%s, value2 = %s", values, value2) : values;

        String path = "";
        if (isBinQualifier(qualifier)) { // bin qualifier
            path = (hasLength(qualifier.getBinName()) ? String.format(" path = %s,", qualifier.getBinName()) : "");
            if (qualifier.getCtxArray() != null && qualifier.getCtxArray().length > 0) {
                path += "." + ctxArrToString(qualifier.getCtxArray());
            }
            if (qualifier.getKey() != null && hasLength(qualifier.getKey().getObject().toString())) {
                path += "." + qualifier.getKey().getObject().toString();
            }
        } else if (isMetadataQualifier(qualifier)) {
            path = qualifier.getMetadataField().toString();
        }

        String qualifiersStr = (qualifier.getQualifiers() != null && qualifier.getQualifiers().length > 0)
            ? String.format(", qualifiers = %s,", qualifiersHashesToString(qualifier.getQualifiers()))
            : "";

        logger.debug("Created qualifier #{}:{} operation = {}{}{}", qualifier.hashCode(), path, strOperation, values,
            qualifiersStr);
    }

    private static String qualifiersHashesToString(@NonNull Qualifier[] qualifiers) {
        return "[" + String.join(",",
            Arrays.stream(qualifiers).map(qualifier -> String.valueOf(qualifier.hashCode())).toList())
            + "]";
    }

    private static boolean isBinQualifier(Qualifier qualifier) {
        return qualifier.getBinName() != null;
    }

    private static boolean isMetadataQualifier(Qualifier qualifier) {
        return qualifier.getMetadataField() != null;
    }

    public static String valueToString(Value value) {
        if (value != null && hasLength(value.toString())) return value.toString();
        return value == Value.getAsNull() ? "null" : "";
    }

    public static <T> List<T> iterableToList(Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false)
            .collect(Collectors.toList());
    }

    /**
     * Given a String in camel case generate its variants in different formats: kebab case, snake case, Pascal case and
     * upper case
     *
     * @param input String in a camel case
     * @return List with 5 variants of the given String, or an empty List if input is null or empty
     */
    public static List<String> getInDifferentFormats(String input) {
        if (!StringUtils.hasText(input)) return List.of();

        List<String> variants = new ArrayList<>();

        // Camel case (already in the original form)
        variants.add(input);

        // Kebab case (convert camel case to kebab case)
        variants.add(input.replaceAll("([a-z])([A-Z])", "$1-$2").toLowerCase());

        // Snake case (convert camel case to snake case)
        variants.add(input.replaceAll("([a-z])([A-Z])", "$1_$2").toLowerCase());

        // Uppercase (convert to uppercase with underscores)
        variants.add(input.replaceAll("([a-z])([A-Z])", "$1_$2").toUpperCase());

        // Pascal case (capitalize the first letter of the camel case)
        variants.add(input.substring(0, 1).toUpperCase() + input.substring(1));

        return variants;
    }

    /**
     * Get value of a property by providing its prefix and name in the environment
     *
     * @param environment  - Actual environment
     * @param prefix       Property prefix
     * @param propertyName Property name in camel case, will be also checked in kebab case, snake case and
     *                     underscore-separated upper case
     * @return The value of the property or null
     */
    public static String getProperty(Environment environment, String prefix, String propertyName) {
        for (String property : getInDifferentFormats(propertyName)) {
            property = prefix + "." + property;
            if (environment.containsProperty(property)) {
                return environment.getProperty(property);
            }
        }
        return null;
    }

    /**
     * Set String value of the given property if it is found in external configuration
     *
     * @param setter       Setter method reference
     * @param environment  Actual environment
     * @param prefix       Property prefix
     * @param propertyName Property name in camel case
     */
    public static void setStringFromConfig(Consumer<String> setter, Environment environment, String prefix,
                                           String propertyName) {
        String value = getProperty(environment, prefix, propertyName);
        if (value != null) setter.accept(value);
    }

    /**
     * Set boolean value of the given property if it is found in external configuration
     *
     * @param setter       Setter method reference
     * @param environment  Actual environment
     * @param prefix       Property prefix
     * @param propertyName Property name in camel case
     */
    public static void setBoolFromConfig(Consumer<Boolean> setter, Environment environment, String prefix,
                                         String propertyName) {
        String value = getProperty(environment, prefix, propertyName);
        if (value != null) setter.accept(Boolean.parseBoolean(value));
    }

    /**
     * Set integer value of the given property if it is found in external configuration
     *
     * @param setter       Setter method reference
     * @param environment  Actual environment
     * @param prefix       Property prefix
     * @param propertyName Property name in camel case
     */
    public static void setIntFromConfig(Consumer<Integer> setter, Environment environment, String prefix,
                                        String propertyName) {
        String value = getProperty(environment, prefix, propertyName);
        if (value != null) setter.accept(Integer.parseInt(value));
    }
}
