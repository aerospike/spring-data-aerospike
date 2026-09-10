package org.springframework.data.aerospike.examples.reactive.converters.entity;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;

public class ReactiveConverterOrderId {

    private String accountId;
    private long orderNumber;

    public ReactiveConverterOrderId() {
    }

    public ReactiveConverterOrderId(String accountId, long orderNumber) {
        this.accountId = accountId;
        this.orderNumber = orderNumber;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public long getOrderNumber() {
        return orderNumber;
    }

    public void setOrderNumber(long orderNumber) {
        this.orderNumber = orderNumber;
    }

    @WritingConverter
    public enum ReactiveConverterOrderIdToStringConverter implements Converter<ReactiveConverterOrderId, String> {
        INSTANCE;

        @Override
        public String convert(ReactiveConverterOrderId source) {
            return source.accountId + "::" + source.orderNumber;
        }
    }

    @ReadingConverter
    public enum StringToReactiveConverterOrderIdConverter implements Converter<String, ReactiveConverterOrderId> {
        INSTANCE;

        @Override
        public ReactiveConverterOrderId convert(String source) {
            String[] parts = source.split("::", 2);
            return new ReactiveConverterOrderId(parts[0], Long.parseLong(parts[1]));
        }
    }
}
