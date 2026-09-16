package org.springframework.data.aerospike.examples.blocking.pagination.entity;

import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.annotation.Id;

// tag::pagination-entity[]
@Document(collection = "sda_examples_pagination_movies")
public class PaginationMovieDocument {

    public static final String GENRE_INDEX = "sda_examples_pagination_genre_idx";
    public static final String RELEASE_YEAR_INDEX = "sda_examples_pagination_year_idx";

    @Id
    private String id;
    private String title;
    private String genre;
    private int releaseYear;

    public PaginationMovieDocument() {
    }

    public PaginationMovieDocument(String id, String title, String genre, int releaseYear) {
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
// end::pagination-entity[]
