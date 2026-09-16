package org.springframework.data.aerospike.examples.reactive.querymethods;

import org.springframework.data.aerospike.examples.reactive.querymethods.entity.ReactiveQueryMethodsMovieDocument;
import org.springframework.data.aerospike.examples.reactive.querymethods.repository.ReactiveQueryMethodsMovieRepository;

import java.util.Comparator;
import java.util.List;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;
import static org.springframework.data.aerospike.examples.support.ExampleCollections.toSortedList;
import static org.springframework.data.aerospike.query.QueryParam.of;

// Demonstrates reactive derived queries that combine id criteria with regular bins.
public class ReactiveRepositoryDerivedIdAndBinQueryExample {

    private final ReactiveQueryMethodsMovieRepository repository;

    public ReactiveRepositoryDerivedIdAndBinQueryExample(ReactiveQueryMethodsMovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        repository.saveAll(List.of(
                new ReactiveQueryMethodsMovieDocument("reactive-id-bin-1", "Arrival", "science-fiction", 2016),
                new ReactiveQueryMethodsMovieDocument("reactive-id-bin-2", "Annihilation", "science-fiction", 2018),
                new ReactiveQueryMethodsMovieDocument("reactive-id-bin-3", "Memories of Murder", "crime", 2003)
            ))
            .collectList()
            .block();

        // tag::reactive-derived-query-id-bin-usage[]
        // QueryParam lets a derived id criterion combine one id with a regular bin criterion.
        List<ReactiveQueryMethodsMovieDocument> singleIdAndGenre = toSortedList(
            repository.findByIdAndGenre(of("reactive-id-bin-1"), of("science-fiction"))
                .collectList()
                .block(),
            Comparator.comparing(ReactiveQueryMethodsMovieDocument::getId), "Expected a list of movies");

        // The same method can narrow several ids before applying the genre criterion.
        List<ReactiveQueryMethodsMovieDocument> idsAndGenre = toSortedList(
            repository.findByIdAndGenre(
                    of(List.of("reactive-id-bin-1", "reactive-id-bin-3")),
                    of("science-fiction"))
                .collectList()
                .block(),
            Comparator.comparing(ReactiveQueryMethodsMovieDocument::getId), "Expected a list of movies");
        // end::reactive-derived-query-id-bin-usage[]

        require(singleIdAndGenre.size() == 1, "Expected one reactive movie matching id and genre");
        require("Arrival".equals(singleIdAndGenre.get(0).getTitle()),
            "Reactive single id and genre query title did not match");
        require(idsAndGenre.size() == 1, "Expected reactive ids and genre to retain only the science-fiction id");
        require("Arrival".equals(idsAndGenre.get(0).getTitle()), "Reactive ids and genre query title did not match");

        System.out.println("Ran reactive derived queries combining id criteria with a genre bin");
    }
}
