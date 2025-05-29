package org.springframework.data.aerospike.core;

import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.springframework.data.aerospike.query.qualifier.Qualifier.and;
import static org.springframework.data.aerospike.query.qualifier.Qualifier.or;

public final class QualifierUtils {

    private QualifierUtils() {
        throw new UnsupportedOperationException("Utility class QualifierUtils cannot be instantiated");
    }

    /**
     * Retrieves the ID value(s) from a given {@code Qualifier} object. This method expects the qualifier to contain an
     * ID. If no ID is present, an {@code IllegalArgumentException} is thrown.
     *
     * @param qualifier The {@link Qualifier} object from which to extract the ID. Must not be null and must contain an
     *                  ID
     * @return A {@code List<Object>} containing the ID value(s). The list will contain a single element if the ID is a
     * simple object or a byte array, or multiple elements if the ID is an array or an iterable
     * @throws IllegalArgumentException If the provided {@code qualifier} does not contain an ID
     */
    public static List<Object> getIdValue(Qualifier qualifier) {
        if (qualifier.hasId()) {
            return idObjectToList(qualifier.getId());
        } else {
            throw new IllegalArgumentException("Id qualifier must contain value");
        }
    }

    /**
     * Converts a given object representing one or more IDs into a {@code List<Object>}. This method handles various
     * types of ID inputs:
     * <ul>
     * <li>If {@code ids} is a {@code byte[]}, it is treated as a single ID object.</li>
     * <li>If {@code ids} is any other array type (e.g., {@code Object[]}, {@code String[]}),
     * its elements are streamed and collected.</li>
     * <li>If {@code ids} is an {@code Iterable<?>} (including {@code Collection}),
     * its elements are streamed and collected. Each element is mapped to {@code Object}
     * to ensure type safety.</li>
     * <li>For any other type, {@code ids} is treated as a single ID object.</li>
     * </ul>
     * The resulting list is immutable.
     *
     * @param ids The object representing the ID(s). Must not be null
     * @return An immutable {@code List<Object>} containing the processed ID(s)
     * @throws IllegalArgumentException If {@code ids} is null
     */
    private static List<Object> idObjectToList(Object ids) {
        Assert.notNull(ids, "Ids must not be null");
        Stream<Object> idStream;
        if (ids instanceof byte[]) {
            // Special case: treat byte[] as a single ID object
            idStream = Stream.of(ids);
        } else if (ids.getClass().isArray()) {
            // Handle all other array types (e.g., Object[], String[], Integer[])
            idStream = Arrays.stream((Object[]) ids);
        } else if (ids instanceof Iterable<?> iterable) {
            // Handle all iterable types (including Collection, as Collection extends Iterable)
            // The mapping is needed to safely cast elements from Iterable<?> to Object
            idStream = StreamSupport.stream(iterable.spliterator(), false)
                .map(o -> (Object) o);
        } else {
            // Default case: treat as a single ID object
            idStream = Stream.of(ids);
        }
        // Collect into an immutable List
        return idStream.toList();
    }

    /**
     * Recursively filters an array of {@link Qualifier} objects, excluding any qualifiers that contain an ID. If a
     * qualifier contains nested qualifiers, this method processes those nested qualifiers recursively.
     *
     * @param qualifiers An array of {@link Qualifier} objects to filter. Can be null or empty
     * @return A new array of {@code Qualifier} objects with ID-containing qualifiers removed Returns {@code null} if
     * the input array is null or empty, or if all qualifiers are filtered out
     */
    private static Qualifier[] excludeIdQualifier(Qualifier[] qualifiers) {
        List<Qualifier> qualifiersWithoutId = new ArrayList<>();
        if (qualifiers != null && qualifiers.length > 0) {
            for (Qualifier qualifier : qualifiers) {
                if (qualifier.hasQualifiers()) {
                    Qualifier[] internalQuals = excludeIdQualifier(qualifier.getQualifiers());
                    qualifiersWithoutId.add(combineMultipleQualifiers(qualifier.getOperation(), internalQuals));
                } else if (!qualifier.hasId()) {
                    qualifiersWithoutId.add(qualifier);
                }
            }
            return qualifiersWithoutId.toArray(Qualifier[]::new);
        }
        return null;
    }

    /**
     * Filters a single {@link Qualifier} object, removing any ID-containing qualifiers from its structure, including
     * nested qualifiers. If the input qualifier itself contains an ID and no other nested qualifiers, the method
     * returns null. If the input qualifier contains nested qualifiers, the method recursively processes them.
     *
     * @param qualifier The {@link Qualifier} object to filter. Can be null
     * @return A new {@code Qualifier} object with ID-containing parts removed, or {@code null} if the original
     * qualifier only contained an ID or if all its parts were filtered out
     */
    public static Qualifier excludeIdQualifier(Qualifier qualifier) {
        List<Qualifier> qualifiersWithoutId = new ArrayList<>();
        if (qualifier != null && qualifier.hasQualifiers()) {
            for (Qualifier innerQual : qualifier.getQualifiers()) {
                if (innerQual.hasQualifiers()) {
                    Qualifier[] internalQuals = excludeIdQualifier(innerQual.getQualifiers());
                    qualifiersWithoutId.add(combineMultipleQualifiers(innerQual.getOperation(), internalQuals));
                } else if (!innerQual.hasId()) {
                    qualifiersWithoutId.add(innerQual);
                }
            }
            return combineMultipleQualifiers(qualifier.getOperation() != null ? qualifier.getOperation() :
                FilterOperation.AND, qualifiersWithoutId.toArray(Qualifier[]::new));
        } else if (qualifier != null && qualifier.hasId()) {
            return null;
        }
        return qualifier;
    }

    /**
     * Combines multiple {@code Qualifier} objects into a single compound qualifier using the specified
     * {@code FilterOperation} ({@code AND} / {@code OR}).
     *
     * @param operation  The {@link FilterOperation} to use for combining (e.g., {@link FilterOperation#AND},
     *                   {@link FilterOperation#OR})
     * @param qualifiers An array of {@link Qualifier} objects to combine
     * @return A new Qualifier object representing the combined qualifiers
     * @throws UnsupportedOperationException If the provided operation is not {@code OR} or {@code AND}
     */
    private static Qualifier combineMultipleQualifiers(FilterOperation operation, Qualifier[] qualifiers) {
        if (operation == FilterOperation.OR) {
            return or(qualifiers);
        } else if (operation == FilterOperation.AND) {
            return and(qualifiers);
        }
        throw new UnsupportedOperationException("Only OR / AND operations are supported");
    }
}
