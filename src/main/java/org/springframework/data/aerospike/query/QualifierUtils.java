package org.springframework.data.aerospike.query;

import lombok.experimental.UtilityClass;
import org.springframework.data.aerospike.repository.query.AerospikeCriteria;

import java.util.ArrayList;
import java.util.List;

@UtilityClass
public class QualifierUtils {

    @Deprecated(since = "4.6.0", forRemoval = true)
    public static Qualifier getIdQualifier(AerospikeCriteria criteria) {
        Object qualifiers = getQualifiers(criteria);
        return getOneIdQualifier((Qualifier[]) qualifiers);
    }

    public static Qualifier getIdQualifier(Qualifier criteria) {
        Object qualifiers = getQualifiers(criteria);
        return getOneIdQualifier((Qualifier[]) qualifiers);
    }

    @Deprecated(since = "4.6.0", forRemoval = true)
    public static Qualifier[] getQualifiers(AerospikeCriteria criteria) {
        if (criteria == null) {
            return null;
        } else if (criteria.getQualifiers() == null) {
            return new Qualifier[]{(criteria)};
        }
        return criteria.getQualifiers();
    }

    public static Qualifier[] getQualifiers(Qualifier criteria) {
        if (criteria == null) {
            return null;
        } else if (criteria.getQualifiers() == null) {
            return new Qualifier[]{(criteria)};
        }
        return criteria.getQualifiers();
    }

    public static void validateQualifiers(Qualifier... qualifiers) {
        boolean haveInternalQualifiers = qualifiers.length > 1;
        for (Qualifier qualifier : qualifiers) {
            haveInternalQualifiers = haveInternalQualifiers || qualifier.hasQualifiers();
            // excludeFilter in the upmost parent qualifier is set to true
            // if there are multiple qualifiers
            // must not build secondary index filter based on any of them
            // as it might conflict with the combination of qualifiers
            qualifier.setExcludeFilter(haveInternalQualifiers);
        }
    }

    /**
     * Find id qualifier.
     *
     * @param qualifiers Qualifiers to search through
     * @return The only id qualifier or null.
     * @throws IllegalArgumentException if more than one id qualifier given
     */
    public static Qualifier getOneIdQualifier(Qualifier... qualifiers) {
        if (qualifiers != null && qualifiers.length > 0) {
            List<Qualifier> idQualifiers = getIdQualifiers(qualifiers);
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
}
