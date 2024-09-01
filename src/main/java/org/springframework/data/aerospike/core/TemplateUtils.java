package org.springframework.data.aerospike.core;

import lombok.experimental.UtilityClass;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.mapping.BasicAerospikePersistentEntity;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.springframework.data.aerospike.query.qualifier.Qualifier.and;
import static org.springframework.data.aerospike.query.qualifier.Qualifier.or;

@UtilityClass
public class TemplateUtils {

    final String SERVER_VERSION_6 = "6.0.0";

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
            if (ids instanceof byte[]) {
                result = List.of(ids);
            } else {
                result = Arrays.stream(((Object[]) ids)).toList();
            }
        } else if (ids instanceof Collection<?>) {
            result = new ArrayList<>((Collection<?>) ids);
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

    private static Qualifier combineMultipleQualifiers(FilterOperation operation, Qualifier[] qualifiers) {
        if (operation == FilterOperation.OR) {
            return or(qualifiers);
        } else if (operation == FilterOperation.AND) {
            return and(qualifiers);
        } else {
            throw new UnsupportedOperationException("Only OR / AND operations are supported");
        }
    }

    public static String[] getBinNamesFromTargetClass(Class<?> targetClass,
                                                      MappingContext<BasicAerospikePersistentEntity<?>,
                                                          AerospikePersistentProperty> mappingContext) {
        AerospikePersistentEntity<?> targetEntity = mappingContext.getRequiredPersistentEntity(targetClass);

        List<String> binNamesList = new ArrayList<>();

        targetEntity.doWithProperties((PropertyHandler<AerospikePersistentProperty>) property
            -> {
            if(!property.isIdProperty()) {
                binNamesList.add(property.getFieldName());
            }
        });

        return binNamesList.toArray(new String[0]);
    }

}
