package org.springframework.data.aerospike.query;

import lombok.experimental.UtilityClass;
import org.springframework.data.aerospike.repository.query.Query;

import java.util.ArrayList;
import java.util.List;

@UtilityClass
public class QualifierUtils {

    public static Qualifier getIdQualifier(Qualifier qualifier) {
        return getOneIdQualifier(qualifier);
    }

    /**
     * Find id qualifier.
     *
     * @param qualifier {@link Qualifier} to search through
     * @return The only id qualifier or null
     * @throws IllegalArgumentException if more than one id qualifier given
     */
    public static Qualifier getOneIdQualifier(Qualifier qualifier) {
        if (qualifier != null) {
            List<Qualifier> idQualifiers = getIdQualifiers(new Qualifier[]{qualifier});
            if (idQualifiers.size() > 1) {
                throw new IllegalArgumentException("Expecting not more than one id qualifier in qualifiers array," +
                    " got " + idQualifiers.size());
            } else if (idQualifiers.size() == 1) {
                return idQualifiers.get(0);
            }
        }
        return null;
    }

    private static List<Qualifier> getIdQualifiers(Qualifier[] qualifiers) {
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

    public static boolean queryCriteriaIsNotNull(Query query) {
        return query != null && query.getCriteria() != null;
    }
}
