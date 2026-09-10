package org.springframework.data.aerospike.examples.reactive.customquery;

import org.springframework.data.aerospike.examples.reactive.customquery.dto.ReactiveProgrammaticMovieSummary;
import org.springframework.data.aerospike.examples.reactive.customquery.entity.ReactiveProgrammaticCustomQueryMovieDocument;
import org.springframework.data.aerospike.examples.reactive.customquery.repository.ReactiveProgrammaticCustomQueryMovieRepository;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;
import static org.springframework.data.aerospike.examples.support.ExampleCollections.toSortedList;

@Component
public class ReactiveProgrammaticCustomQueryExample {

    private final ReactiveProgrammaticCustomQueryMovieRepository repository;

    // This repository uses inherited findUsingQuery(...) methods instead of @Query annotations
    public ReactiveProgrammaticCustomQueryExample(ReactiveProgrammaticCustomQueryMovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        repository.saveAll(List.of(
                new ReactiveProgrammaticCustomQueryMovieDocument(
                    "reactive-programmatic-query-1", "Edge of Tomorrow", "science-fiction", 2014),
                new ReactiveProgrammaticCustomQueryMovieDocument(
                    "reactive-programmatic-query-2", "Ex Machina", "science-fiction", 2014),
                new ReactiveProgrammaticCustomQueryMovieDocument(
                    "reactive-programmatic-query-3", "No Country for Old Men", "crime", 2007),
                new ReactiveProgrammaticCustomQueryMovieDocument(
                    "reactive-programmatic-query-4", "The Batman", "crime", 2022),
                new ReactiveProgrammaticCustomQueryMovieDocument(
                    "reactive-programmatic-query-5", "Nightcrawler", "crime", 2014),
                new ReactiveProgrammaticCustomQueryMovieDocument(
                    "reactive-programmatic-query-6", "Arrival", "science-fiction", 2016)
            ))
            .collectList()
            .block();

        Qualifier scienceFiction = Qualifier.builder()
            .setPath("genre")
            .setFilterOperation(FilterOperation.EQ)
            .setValue("science-fiction")
            .build();

        Qualifier releasedIn2014 = Qualifier.builder()
            .setPath("releaseYear")
            .setFilterOperation(FilterOperation.BETWEEN)
            .setValue(2014)
            .setSecondValue(2015)
            .build();

        // AND combines explicit Qualifier objects. Both bins are indexed by the example fixture
        Query scienceFictionFrom2014 = new Query(Qualifier.and(scienceFiction, releasedIn2014));

        List<ReactiveProgrammaticCustomQueryMovieDocument> matches =
            toSortedList(repository.findUsingQuery(scienceFictionFrom2014).collectList().block(),
                Comparator.comparing(ReactiveProgrammaticCustomQueryMovieDocument::getId),
                "Expected a list of movies");
        require(matches.size() == 2, "Expected two movies from the programmatic query");
        require("Edge of Tomorrow".equals(matches.get(0).getTitle()), "First programmatic query title did not match");
        require("Ex Machina".equals(matches.get(1).getTitle()), "Second programmatic query title did not match");

        // The target-class overload maps matching records into a projection DTO
        List<ReactiveProgrammaticMovieSummary> summaries = toSortedList(repository
            .findUsingQuery(scienceFictionFrom2014, ReactiveProgrammaticMovieSummary.class)
            .collectList()
            .block(), Comparator.comparing(ReactiveProgrammaticMovieSummary::getTitle),
            "Expected a list of summaries");
        require(summaries.size() == 2, "Expected two projected programmatic query results");
        require("Edge of Tomorrow".equals(summaries.get(0).getTitle()), "First projected title did not match");
        require(summaries.get(0).getReleaseYear() == 2014, "First projected release year did not match");

        System.out.println("Ran a reactive programmatic repository query with Qualifier and DTO projection");
    }

}
