package org.springframework.data.aerospike.utility;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;

@RequiredArgsConstructor
public class MockObjectProvider<T> implements ObjectProvider<T> {

    private final T obj;

    @Override
    public T getObject(Object... args) throws BeansException {
        return obj;
    }

    @Override
    public T getIfAvailable() throws BeansException {
        return obj;
    }

    @Override
    public T getIfUnique() throws BeansException {
        return obj;
    }

    @Override
    public T getObject() throws BeansException {
        return obj;
    }
}
