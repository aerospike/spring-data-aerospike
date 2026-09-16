package org.springframework.data.aerospike.examples.blocking.customquery.types.entity;

import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.annotation.Id;

// tag::custom-query-types-entity[]
@Document(collection = "sda_examples_custom_query_type_movies")
public class CustomQueryTypesMovieDocument {

    public static final String RELEASE_YEAR_INDEX = "sda_examples_custom_query_type_year_idx";
    public static final String EXPRESSION_INDEX = "sda_examples_custom_query_type_expr_idx";

    @Id
    private String id;
    private String title;
    private String genre;
    private String director;
    private int releaseYear;

    public CustomQueryTypesMovieDocument() {
    }

    public CustomQueryTypesMovieDocument(String id, String title, String genre, String director, int releaseYear) {
        this.id = id;
        this.title = title;
        this.genre = genre;
        this.director = director;
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

    public String getDirector() {
        return director;
    }

    public void setDirector(String director) {
        this.director = director;
    }

    public int getReleaseYear() {
        return releaseYear;
    }

    public void setReleaseYear(int releaseYear) {
        this.releaseYear = releaseYear;
    }
}
// end::custom-query-types-entity[]
