package org.springframework.data.aerospike.examples.reactive.template.dto;

public class ReactiveTemplateMovieSummary {

    private String title;
    private int releaseYear;

    public ReactiveTemplateMovieSummary() {
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
