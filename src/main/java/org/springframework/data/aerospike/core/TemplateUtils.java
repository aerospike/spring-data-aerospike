package org.springframework.data.aerospike.core;

import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class TemplateUtils {

    public static Collection<Object> getIdValue(Qualifier... qualifiers) {
        return Arrays.stream(qualifiers).filter(Qualifier::hasId)
            .filter(qualifier -> qualifier.getValue1() != null)
            .map(qualifier -> idObjectToCollection(qualifier.getValue1().getObject()))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Value of 'id' field in a Qualifier was not found"));
    }

    private static Collection<Object> idObjectToCollection(Object ids) {
        Collection<Object> result;
        Assert.notNull(ids, "Ids must not be null");
        if (ids.getClass().isArray()) {
            result = Arrays.stream(((Object[]) ids)).toList();
        } else if (ids instanceof Collection<?>) {
            result = ((Collection<Object>) ids);
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
                    Qualifier.QualifierBuilder qb = Qualifier.builder().setFilterOperation(qualifier.getOperation());
                    qualifiersWithoutId.add(qb.setQualifiers(internalQuals).build());
                } else if (!qualifier.hasId()) {
                    qualifiersWithoutId.add(qualifier);
                }
            }
            return qualifiersWithoutId.toArray(Qualifier[]::new);
        }
        return null;
    }
}
