package org.springframework.data.aerospike.examples.logical.dto;

import org.springframework.data.aerospike.examples.logical.entity.LogicalMovieDocument;
import org.springframework.data.aerospike.mapping.Field;

public class LogicalMovieSummary {

    @Field(LogicalMovieDocument.TITLE_BIN)
    private String title;
    @Field(LogicalMovieDocument.RELEASE_YEAR_BIN)
    private int releaseYear;

    public LogicalMovieSummary() {
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
