package org.springframework.data.aerospike.examples.blocking.caching.entity;

import org.springframework.data.annotation.Id;

// tag::caching-value[]
public class CachedMovie {

    @Id
    private String id;
    private String title;

    public CachedMovie() {
    }

    public CachedMovie(String id, String title) {
        this.id = id;
        this.title = title;
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
}
// end::caching-value[]
