package org.springframework.data.aerospike.query;

import lombok.experimental.UtilityClass;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.domain.Sort;

import java.util.ArrayList;
import java.util.List;

@UtilityClass
public class QualifierUtils {

    /**
     * Find id qualifier.
     *
     * @param qualifier {@link Qualifier} to search through
     * @return The only id qualifier or null
     * @throws IllegalArgumentException if more than one id qualifier given
     */
    public static Qualifier getIdQualifier(Qualifier qualifier) {
        if (qualifier != null) {
            List<Qualifier> idQualifiers = getIdQualifiers(qualifier);
            if (idQualifiers.size() > 1) {
                throw new IllegalArgumentException("Expecting not more than one id qualifier in qualifiers array," +
                    " got " + idQualifiers.size());
            } else if (idQualifiers.size() == 1) {
                return idQualifiers.get(0);
            }
        }
        return null;
    }

    /**
     * Recursively extracts and collects all {@code Qualifier} objects that contain an ID from a given array of qualifiers.
     * If a qualifier itself does not have an ID, but contains nested qualifiers, this method will recursively
     * process those nested qualifiers.
     *
     * @param qualifiers An array of {@code Qualifier} objects to be inspected. Can be empty or contain null elements,
     * null elements will be ignored
     * @return A {@code List} of id qualifiers, or an empty list if no id qualifiers are found,
     * or if the input array is empty
     */
    private static List<Qualifier> getIdQualifiers(Qualifier... qualifiers) {
        List<Qualifier> idQualifiers = new ArrayList<>();
        for (Qualifier qualifier : qualifiers) {
            if (qualifier.hasId()) {
                idQualifiers.add(qualifier);
            } else {
                if (qualifier.hasQualifiers()) {
                    idQualifiers.addAll(getIdQualifiers(qualifier.getQualifiers()));
                }
            }
        }
        return idQualifiers;
    }

    /**
     * Checks if a given {@code Query} object and its criteria are not null.
     *
     * @param query The {@code Query} object to check
     * @return {@code true} if the query object is not null AND its criteria object is not null; {@code false} otherwise
     */
    public static boolean isQueryCriteriaNotNull(Query query) {
        return query != null && query.getCriteria() != null;
    }

    /**
     * Determines if a given {@code Query} is {@code null} or if its {@code Sort} component is explicitly
     * {@code Sort.unsorted()}.
     *
     * @param query The {@code Query} object to check
     * @return {@code true} if the query is null or if its sort order is unsorted; {@code false} otherwise
     */
    public static boolean isQueryNullOrUnsorted(Query query) {
        return query == null || query.getSort() == Sort.unsorted();
    }
}
