package org.springframework.data.aerospike.examples.combined.dto;

import org.springframework.data.aerospike.examples.combined.entity.Movie;
import org.springframework.data.aerospike.mapping.Field;

public class MovieSummary {

    @Field(Movie.TITLE_BIN)
    private String title;
    @Field(Movie.RELEASE_YEAR_BIN)
    private int releaseYear;

    public MovieSummary() {
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public int getReleaseYear() {
        return releaseYear;
    }

    public void setReleaseYear(int releaseYear) {
        this.releaseYear = releaseYear;
    }
}
