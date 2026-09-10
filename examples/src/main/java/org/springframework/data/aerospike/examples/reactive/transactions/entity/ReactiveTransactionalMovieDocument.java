package org.springframework.data.aerospike.examples.reactive.transactions.entity;

import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.annotation.Id;

@Document(collection = "sda_examples_reactive_transaction_movies")
public class ReactiveTransactionalMovieDocument {

    @Id
    private String id;
    private String title;
    private String status;

    public ReactiveTransactionalMovieDocument() {
    }

    public ReactiveTransactionalMovieDocument(String id, String title, String status) {
        this.id = id;
        this.title = title;
        this.status = status;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
