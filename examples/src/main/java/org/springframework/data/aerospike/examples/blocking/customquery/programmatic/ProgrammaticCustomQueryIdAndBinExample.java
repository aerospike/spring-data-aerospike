package org.springframework.data.aerospike.examples.blocking.customquery.programmatic;

import org.springframework.data.aerospike.examples.blocking.customquery.programmatic.entity.ProgrammaticCustomQueryMovieDocument;
import org.springframework.data.aerospike.examples.blocking.customquery.programmatic.repository.ProgrammaticCustomQueryMovieRepository;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;

import java.util.Comparator;
import java.util.List;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;
import static org.springframework.data.aerospike.examples.support.ExampleCollections.toSortedList;

// Demonstrates blocking custom queries that combine id qualifiers with regular bin qualifiers.
public class ProgrammaticCustomQueryIdAndBinExample {

    private final ProgrammaticCustomQueryMovieRepository repository;

    public ProgrammaticCustomQueryIdAndBinExample(ProgrammaticCustomQueryMovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        repository.saveAll(List.of(
            new ProgrammaticCustomQueryMovieDocument("blocking-custom-id-bin-1", "Stalker", "science-fiction", 1979),
            new ProgrammaticCustomQueryMovieDocument("blocking-custom-id-bin-2", "Solaris", "science-fiction", 1972),
            new ProgrammaticCustomQueryMovieDocument("blocking-custom-id-bin-3", "High and Low", "crime", 1963)
        ));

        // tag::blocking-custom-query-id-bin-usage[]
        // Qualifier.idEquals targets the Aerospike user key; genreQualifier targets a regular bin.
        Query singleIdAndGenre = new Query(Qualifier.and(
            Qualifier.idEquals("blocking-custom-id-bin-1"),
            genreQualifier("science-fiction")));

        List<ProgrammaticCustomQueryMovieDocument> singleIdResult = toSortedList(
            repository.findUsingQuery(singleIdAndGenre),
            Comparator.comparing(ProgrammaticCustomQueryMovieDocument::getId));

        // Qualifier.idIn lets the same custom query shape narrow several ids before applying the bin qualifier.
        Query idsAndGenre = new Query(Qualifier.and(
            Qualifier.idIn("blocking-custom-id-bin-1", "blocking-custom-id-bin-3"),
            genreQualifier("science-fiction")));

        List<ProgrammaticCustomQueryMovieDocument> idsResult = toSortedList(
            repository.findUsingQuery(idsAndGenre),
            Comparator.comparing(ProgrammaticCustomQueryMovieDocument::getId));
        // end::blocking-custom-query-id-bin-usage[]

        require(singleIdResult.size() == 1, "Expected one movie matching custom id and genre query");
        require("Stalker".equals(singleIdResult.get(0).getTitle()), "Custom single id and genre title did not match");
        require(idsResult.size() == 1, "Expected custom ids and genre query to retain only the science-fiction id");
        require("Stalker".equals(idsResult.get(0).getTitle()), "Custom ids and genre title did not match");

        System.out.println("Ran blocking custom queries combining id qualifiers with a genre bin");
    }

    private Qualifier genreQualifier(String genre) {
        return Qualifier.builder()
            .setPath("genre")
            .setFilterOperation(FilterOperation.EQ)
            .setValue(genre)
            .build();
    }
}
