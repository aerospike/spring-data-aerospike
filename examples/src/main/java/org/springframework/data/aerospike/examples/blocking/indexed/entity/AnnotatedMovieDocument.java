package org.springframework.data.aerospike.examples.blocking.indexed.entity;

import com.aerospike.client.query.IndexType;
import org.springframework.data.aerospike.annotation.Indexed;
import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.annotation.Id;

@Document(collection = "sda_examples_indexed_movies")
public class AnnotatedMovieDocument {

    public static final String GENRE_INDEX = "sda_examples_indexed_genre_idx";

    @Id
    private String id;
    private String title;
    @Indexed(type = IndexType.STRING, name = GENRE_INDEX)
    private String genre;

    public AnnotatedMovieDocument() {
    }

    public AnnotatedMovieDocument(String id, String title, String genre) {
        this.id = id;
        this.title = title;
        this.genre = genre;
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
}
