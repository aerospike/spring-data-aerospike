package org.springframework.data.aerospike.examples.reactive.converters.entity;

import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.annotation.Id;

@Document(collection = "sda_examples_reactive_converter_orders")
public class ReactiveConverterOrderDocument {

    @Id
    private ReactiveConverterOrderId id;
    private String description;
    private int quantity;

    public ReactiveConverterOrderDocument() {
    }

    public ReactiveConverterOrderDocument(ReactiveConverterOrderId id, String description, int quantity) {
        this.id = id;
        this.description = description;
        this.quantity = quantity;
    }

    public ReactiveConverterOrderId getId() {
        return id;
    }

    public void setId(ReactiveConverterOrderId id) {
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
