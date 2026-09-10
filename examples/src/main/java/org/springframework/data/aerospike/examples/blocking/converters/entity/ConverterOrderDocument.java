package org.springframework.data.aerospike.examples.blocking.converters.entity;

import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.annotation.Id;

@Document(collection = "sda_examples_blocking_converter_orders")
public class ConverterOrderDocument {

    @Id
    private ConverterOrderId id;
    private String description;
    private int quantity;

    public ConverterOrderDocument() {
    }

    public ConverterOrderDocument(ConverterOrderId id, String description, int quantity) {
        this.id = id;
        this.description = description;
        this.quantity = quantity;
    }

    public ConverterOrderId getId() {
        return id;
    }

    public void setId(ConverterOrderId id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }
}
