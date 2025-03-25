package org.springframework.data.aerospike.repository.query;

import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.repository.query.parser.Part;

import java.util.Collection;
import java.util.List;

import static org.springframework.data.aerospike.query.qualifier.Qualifier.idEquals;
import static org.springframework.data.aerospike.query.qualifier.Qualifier.idIn;

public class IdQueryCreator implements IAerospikeQueryCreator {

    private final Part.Type partType;
    private final List<Object> queryParameters;

    public IdQueryCreator(Part part, List<Object> queryParameters) {
        this.partType = part.getType();
        this.queryParameters = queryParameters;
    }

    @Override
    public void validate() {
        if (partType != Part.Type.SIMPLE_PROPERTY && partType != Part.Type.LIKE) {
            throw new UnsupportedOperationException(
                String.format("Unsupported type for an id query '%s', only EQ and LIKE id queries are supported",
                    partType)
            );
        }
    }

    @Override
    public Qualifier process() {
        Object value = queryParameters.get(0);
        if (value instanceof Collection<?>) {
            List<?> ids = ((Collection<?>) value).stream().toList();
            return getIdInQualifier(ids);
        } else {
            return getIdEqualsQualifier(value);
        }
    }

    @SuppressWarnings("SuspiciousToArrayCall")
    private Qualifier getIdInQualifier(List<?> ids) {
        Qualifier qualifier;
        Object firstId = ids.get(0);
        if (firstId instanceof String) {
            qualifier = idIn(ids.toArray(String[]::new));
        } else if (firstId instanceof Long) {
            qualifier = idIn(ids.toArray(Long[]::new));
        } else if (firstId instanceof Integer) {
            qualifier = idIn(ids.toArray(Integer[]::new));
        } else if (firstId instanceof Short) {
            qualifier = idIn(ids.toArray(Short[]::new));
        } else if (firstId instanceof Byte) {
            qualifier = idIn(ids.toArray(Byte[]::new));
        } else if (firstId instanceof Character) {
            qualifier = idIn(ids.toArray(Character[]::new));
        } else if (firstId instanceof byte[]) {
            qualifier = idIn(ids.toArray(byte[][]::new));
        } else {
            throw new IllegalArgumentException("Invalid ID argument type: expected String, Number or byte[]");
        }
        return qualifier;
    }

    private Qualifier getIdEqualsQualifier(Object value) {
        Qualifier qualifier;
        if (value instanceof String) {
            qualifier = idEquals((String) value);
        } else if (value instanceof Long) {
            qualifier = idEquals((Long) value);
        } else if (value instanceof Integer) {
            qualifier = idEquals((Integer) value);
        } else if (value instanceof Short) {
            qualifier = idEquals((Short) value);
        } else if (value instanceof Byte) {
            qualifier = idEquals((Byte) value);
        } else if (value instanceof Character) {
            qualifier = idEquals((Character) value);
        } else if (value instanceof byte[]) {
            qualifier = idEquals((byte[]) value);
        } else {
            throw new IllegalArgumentException("Invalid ID argument type: expected String, Number or byte[]");
        }
        return qualifier;
    }
}
