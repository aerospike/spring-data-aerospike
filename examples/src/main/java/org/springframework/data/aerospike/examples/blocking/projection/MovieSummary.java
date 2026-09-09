package org.springframework.data.aerospike.examples.blocking.projection;

public class MovieSummary {

    private String title;
    private int releaseYear;

    public MovieSummary() {
    }

    public MovieSummary(String title, int releaseYear) {
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
