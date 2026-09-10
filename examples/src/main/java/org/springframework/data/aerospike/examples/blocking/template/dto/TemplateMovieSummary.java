package org.springframework.data.aerospike.examples.blocking.template.dto;

public class TemplateMovieSummary {

    private String title;
    private int releaseYear;

    public TemplateMovieSummary() {
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
