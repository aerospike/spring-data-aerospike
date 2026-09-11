package org.springframework.data.aerospike.examples.combined.entity;

import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.aerospike.mapping.Field;
import org.springframework.data.annotation.Id;

@Document(collection = "sda_examples_combined_movies")
public class Movie {

    public static final String GENRE_BIN = "lGenre";
    public static final String RELEASE_YEAR_BIN = "lYear";
    public static final String TITLE_BIN = "lTitle";
    public static final String GENRE_INDEX = "sda_examples_combined_genre_idx";
    public static final String TITLE_INDEX = "sda_examples_combined_title_idx";

    @Id
    private String id;
    @Field(TITLE_BIN)
    private String title;
    @Field(GENRE_BIN)
    private String genre;
    @Field(RELEASE_YEAR_BIN)
    private int releaseYear;

    public Movie() {
    }

    public Movie(String id, String title, String genre, int releaseYear) {
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
