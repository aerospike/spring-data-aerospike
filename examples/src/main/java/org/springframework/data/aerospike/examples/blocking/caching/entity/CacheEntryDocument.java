package org.springframework.data.aerospike.examples.blocking.caching.entity;

import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.annotation.Id;

@Document(collection = "sda_examples_cache_entries")
public class CacheEntryDocument {

    public static final String SET_NAME = "sda_examples_cache_entries";

    @Id
    private String id;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
