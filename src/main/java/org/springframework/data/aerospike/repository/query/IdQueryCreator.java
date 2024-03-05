package org.springframework.data.aerospike.repository.query;

import org.springframework.data.aerospike.query.Qualifier;

import java.util.Collection;
import java.util.List;

import static org.springframework.data.aerospike.query.Qualifier.idEquals;
import static org.springframework.data.aerospike.query.Qualifier.idIn;

public class IdQueryCreator implements IAerospikeQueryCreator {

    private final List<Object> queryParameters;

    public IdQueryCreator(List<Object> queryParameters) {
        this.queryParameters = queryParameters;
    }

    @Override
    public void validate() {
        // in this case validation is done within process()
    }

    @Override
    public Qualifier process() {
        Object value1 = queryParameters.get(0);
        if (value1 instanceof Collection<?>) {
            List<?> ids = ((Collection<?>) value1).stream().toList();
            return getIdInQualifier(ids);
        } else {
            return getIdEqualsQualifier(value1);
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

    private Qualifier getIdEqualsQualifier(Object value1) {
        Qualifier qualifier;
        if (value1 instanceof String) {
            qualifier = idEquals((String) value1);
        } else if (value1 instanceof Long) {
            qualifier = idEquals((Long) value1);
        } else if (value1 instanceof Integer) {
            qualifier = idEquals((Integer) value1);
        } else if (value1 instanceof Short) {
            qualifier = idEquals((Short) value1);
        } else if (value1 instanceof Byte) {
            qualifier = idEquals((Byte) value1);
        } else if (value1 instanceof Character) {
            qualifier = idEquals((Character) value1);
        } else if (value1 instanceof byte[]) {
            qualifier = idEquals((byte[]) value1);
        } else {
            throw new IllegalArgumentException("Invalid ID argument type: expected String, Number or byte[]");
        }
        return qualifier;
    }
}
