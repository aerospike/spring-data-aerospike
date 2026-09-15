package org.springframework.data.aerospike.examples.blocking.querymethods;

import org.springframework.data.aerospike.examples.blocking.querymethods.entity.QueryMethodsMovieDocument;
import org.springframework.data.aerospike.examples.blocking.querymethods.repository.QueryMethodsMovieRepository;

import java.util.Comparator;
import java.util.List;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;
import static org.springframework.data.aerospike.examples.support.ExampleCollections.toSortedList;
import static org.springframework.data.aerospike.query.QueryParam.of;

// Demonstrates blocking derived queries that combine id criteria with regular bins.
public class BlockingRepositoryDerivedIdAndBinQueryExample {

    private final QueryMethodsMovieRepository repository;

    public BlockingRepositoryDerivedIdAndBinQueryExample(QueryMethodsMovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        repository.saveAll(List.of(
            new QueryMethodsMovieDocument("blocking-id-bin-1", "Alien", "science-fiction", 1979),
            new QueryMethodsMovieDocument("blocking-id-bin-2", "Aliens", "science-fiction", 1986),
            new QueryMethodsMovieDocument("blocking-id-bin-3", "Heat", "crime", 1995)
        ));

        // tag::blocking-derived-query-id-bin-usage[]
        // QueryParam lets a derived id criterion combine one id with a regular bin criterion.
        List<QueryMethodsMovieDocument> singleIdAndGenre = toSortedList(
            repository.findByIdAndGenre(of("blocking-id-bin-1"), of("science-fiction")),
            Comparator.comparing(QueryMethodsMovieDocument::getId));

        // The same method can narrow several ids before applying the genre criterion.
        List<QueryMethodsMovieDocument> idsAndGenre = toSortedList(
            repository.findByIdAndGenre(
                of(List.of("blocking-id-bin-1", "blocking-id-bin-3")),
                of("science-fiction")),
            Comparator.comparing(QueryMethodsMovieDocument::getId));
        // end::blocking-derived-query-id-bin-usage[]

        require(singleIdAndGenre.size() == 1, "Expected one movie matching id and genre");
        require("Alien".equals(singleIdAndGenre.get(0).getTitle()), "Single id and genre query title did not match");
        require(idsAndGenre.size() == 1, "Expected ids and genre to retain only the science-fiction id");
        require("Alien".equals(idsAndGenre.get(0).getTitle()), "Ids and genre query title did not match");

        System.out.println("Ran blocking derived queries combining id criteria with a genre bin");
    }
}
