package org.springframework.data.aerospike.examples.blocking.customquery.programmatic.dto;

public class ProgrammaticMovieSummary {

    private String title;
    private int releaseYear;

    public ProgrammaticMovieSummary() {
    }

    public ProgrammaticMovieSummary(String title, int releaseYear) {
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
