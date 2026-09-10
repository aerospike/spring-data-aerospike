package org.springframework.data.aerospike.examples.blocking.transactions.entity;

import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.annotation.Id;

@Document(collection = "sda_examples_blocking_transaction_movies")
public class BlockingTransactionalMovieDocument {

    @Id
    private String id;
    private String title;
    private String status;

    public BlockingTransactionalMovieDocument() {
    }

    public BlockingTransactionalMovieDocument(String id, String title, String status) {
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
