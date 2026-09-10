package org.springframework.data.aerospike.examples.reactive.template.entity;

import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.annotation.Id;

@Document(collection = "sda_examples_reactive_template_movies")
public class ReactiveTemplateMovieDocument {

    public static final String GENRE_INDEX = "sda_examples_reactive_template_genre_idx";
    public static final String RELEASE_YEAR_INDEX = "sda_examples_reactive_template_year_idx";

    @Id
    private String id;
    private String title;
    private String genre;
    private int releaseYear;
    private int rating;
    private long views;

    public ReactiveTemplateMovieDocument() {
    }

    public ReactiveTemplateMovieDocument(String id, String title, String genre, int releaseYear, int rating,
                                         long views) {
        this.id = id;
        this.title = title;
        this.genre = genre;
        this.releaseYear = releaseYear;
        this.rating = rating;
        this.views = views;
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

    public String getGenre() {
        return genre;
    }

    public void setGenre(String genre) {
        this.genre = genre;
    }

    public int getReleaseYear() {
        return releaseYear;
    }

    public void setReleaseYear(int releaseYear) {
        this.releaseYear = releaseYear;
    }

    public int getRating() {
        return rating;
    }

    public void setRating(int rating) {
        this.rating = rating;
    }

    public long getViews() {
        return views;
    }

    public void setViews(long views) {
        this.views = views;
    }
}
