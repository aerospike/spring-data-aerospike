package org.springframework.data.aerospike.examples.reactive.customquery.dto;

public class ReactiveProgrammaticMovieSummary {

    private String title;
    private int releaseYear;

    public ReactiveProgrammaticMovieSummary() {
    }

    public ReactiveProgrammaticMovieSummary(String title, int releaseYear) {
        this.title = title;
        this.releaseYear = releaseYear;
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
