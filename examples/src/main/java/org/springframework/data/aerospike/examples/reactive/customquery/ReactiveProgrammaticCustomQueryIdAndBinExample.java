package org.springframework.data.aerospike.examples.reactive.customquery;

import org.springframework.data.aerospike.examples.reactive.customquery.entity.ReactiveProgrammaticCustomQueryMovieDocument;
import org.springframework.data.aerospike.examples.reactive.customquery.repository.ReactiveProgrammaticCustomQueryMovieRepository;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;

import java.util.Comparator;
import java.util.List;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;
import static org.springframework.data.aerospike.examples.support.ExampleCollections.toSortedList;

// Demonstrates reactive custom queries that combine id qualifiers with regular bin qualifiers.
public class ReactiveProgrammaticCustomQueryIdAndBinExample {

    private final ReactiveProgrammaticCustomQueryMovieRepository repository;

    public ReactiveProgrammaticCustomQueryIdAndBinExample(ReactiveProgrammaticCustomQueryMovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        repository.saveAll(List.of(
                new ReactiveProgrammaticCustomQueryMovieDocument(
                    "reactive-custom-id-bin-1", "Edge of Tomorrow", "science-fiction", 2014),
                new ReactiveProgrammaticCustomQueryMovieDocument(
                    "reactive-custom-id-bin-2", "Ex Machina", "science-fiction", 2014),
                new ReactiveProgrammaticCustomQueryMovieDocument(
                    "reactive-custom-id-bin-3", "No Country for Old Men", "crime", 2007)
            ))
            .collectList()
            .block();

        // tag::reactive-custom-query-id-bin-usage[]
        // Qualifier.idEquals targets the Aerospike user key; genreQualifier targets a regular bin.
        Query singleIdAndGenre = new Query(Qualifier.and(
            Qualifier.idEquals("reactive-custom-id-bin-1"),
            genreQualifier("science-fiction")));

        List<ReactiveProgrammaticCustomQueryMovieDocument> singleIdResult = toSortedList(
            repository.findUsingQuery(singleIdAndGenre)
                .collectList()
                .block(),
            Comparator.comparing(ReactiveProgrammaticCustomQueryMovieDocument::getId),
            "Expected a list of custom query results");

        // Qualifier.idIn lets the same custom query shape narrow several ids before applying the bin qualifier.
        Query idsAndGenre = new Query(Qualifier.and(
            Qualifier.idIn("reactive-custom-id-bin-1", "reactive-custom-id-bin-3"),
            genreQualifier("science-fiction")));

        List<ReactiveProgrammaticCustomQueryMovieDocument> idsResult = toSortedList(
            repository.findUsingQuery(idsAndGenre)
                .collectList()
                .block(),
            Comparator.comparing(ReactiveProgrammaticCustomQueryMovieDocument::getId),
            "Expected a list of custom query results");
        // end::reactive-custom-query-id-bin-usage[]

        require(singleIdResult.size() == 1, "Expected one reactive movie matching custom id and genre query");
        require("Edge of Tomorrow".equals(singleIdResult.get(0).getTitle()),
            "Reactive custom single id and genre title did not match");
        require(idsResult.size() == 1, "Expected reactive custom ids and genre to retain only the science-fiction id");
        require("Edge of Tomorrow".equals(idsResult.get(0).getTitle()), "Reactive custom ids and genre title did not match");

        System.out.println("Ran reactive custom queries combining id qualifiers with a genre bin");
    }

    private Qualifier genreQualifier(String genre) {
        return Qualifier.builder()
            .setPath("genre")
            .setFilterOperation(FilterOperation.EQ)
            .setValue(genre)
            .build();
    }
}
