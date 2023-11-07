package org.springframework.data.aerospike.core;

import lombok.experimental.UtilityClass;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.springframework.data.aerospike.query.Qualifier.and;
import static org.springframework.data.aerospike.query.Qualifier.or;

@UtilityClass
public class TemplateUtils {

    public static List<Object> getIdValue(Qualifier qualifier) {
        if (qualifier.hasId()) {
            return idObjectToList(qualifier.getId());
        } else {
            throw new IllegalArgumentException("Id qualifier must contain value");
        }
    }

    private static List<Object> idObjectToList(Object ids) {
        List<Object> result;
        Assert.notNull(ids, "Ids must not be null");
        if (ids.getClass().isArray()) {
            result = Arrays.stream(((Object[]) ids)).toList();
        } else if (ids instanceof Collection<?>) {
            result = new ArrayList<Object>((Collection) ids);
        } else if (ids instanceof Iterable<?>) {
            result = StreamSupport.stream(((Iterable<?>) ids).spliterator(), false)
                .collect(Collectors.toList());
        } else {
            result = List.of(ids);
        }
        return result;
    }

    public static Qualifier[] excludeIdQualifier(Qualifier[] qualifiers) {
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

    public static Qualifier excludeIdQualifier(Qualifier criteria) {
        List<Qualifier> qualifiersWithoutId = new ArrayList<>();
        if (criteria != null && criteria.hasQualifiers()) {
            for (Qualifier qualifier : criteria.getQualifiers()) {
                if (qualifier.hasQualifiers()) {
                    Qualifier[] internalQuals = excludeIdQualifier(qualifier.getQualifiers());
                    qualifiersWithoutId.add(combineMultipleQualifiers(qualifier.getOperation(), internalQuals));
                } else if (!qualifier.hasId()) {
                    qualifiersWithoutId.add(qualifier);
                }
            }
            return combineMultipleQualifiers(criteria.getOperation() != null ? criteria.getOperation() :
                FilterOperation.AND, qualifiersWithoutId.toArray(Qualifier[]::new));
        } else if (criteria.hasId()) {
            return null;
        }
        return criteria;
    }

    private static Qualifier combineMultipleQualifiers(FilterOperation operation, Qualifier[] qualifiers) {
        if (operation == FilterOperation.OR) {
            return or(qualifiers);
        } else if (operation == FilterOperation.AND) {
            return and(qualifiers);
        } else {
            throw new UnsupportedOperationException("Only OR / AND operations are supported");
        }
    }

    static boolean queryCriteriaIsNotNull(Query query) {
        return query != null && query.getCriteria() != null;
    }
}
