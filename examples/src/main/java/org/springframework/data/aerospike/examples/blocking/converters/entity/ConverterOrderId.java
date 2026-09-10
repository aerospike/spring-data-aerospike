package org.springframework.data.aerospike.examples.blocking.converters.entity;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;

public class ConverterOrderId {

    private String accountId;
    private long orderNumber;

    public ConverterOrderId() {
    }

    public ConverterOrderId(String accountId, long orderNumber) {
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
    public enum ConverterOrderIdToStringConverter implements Converter<ConverterOrderId, String> {
        INSTANCE;

        @Override
        public String convert(ConverterOrderId source) {
            return source.accountId + "::" + source.orderNumber;
        }
    }

    @ReadingConverter
    public enum StringToConverterOrderIdConverter implements Converter<String, ConverterOrderId> {
        INSTANCE;

        @Override
        public ConverterOrderId convert(String source) {
            String[] parts = source.split("::", 2);
            return new ConverterOrderId(parts[0], Long.parseLong(parts[1]));
        }
    }
}
