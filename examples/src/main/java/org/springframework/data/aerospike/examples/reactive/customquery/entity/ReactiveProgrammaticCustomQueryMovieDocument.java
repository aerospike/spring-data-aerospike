package org.springframework.data.aerospike.examples.reactive.customquery.entity;

import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.annotation.Id;

@Document(collection = "sda_examples_reactive_programmatic_query_movies")
public class ReactiveProgrammaticCustomQueryMovieDocument {

    public static final String GENRE_INDEX = "sda_examples_reactive_programmatic_genre_idx";
    public static final String RELEASE_YEAR_INDEX = "sda_examples_reactive_programmatic_year_idx";

    @Id
    private String id;
    private String title;
    private String genre;
    private int releaseYear;

    public ReactiveProgrammaticCustomQueryMovieDocument() {
    }

    public ReactiveProgrammaticCustomQueryMovieDocument(String id, String title, String genre, int releaseYear) {
        this.id = id;
        this.title = title;
        this.genre = genre;
        this.releaseYear = releaseYear;
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
}
