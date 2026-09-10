package org.springframework.data.aerospike.examples.blocking.customquery.programmatic;

import org.springframework.data.aerospike.examples.blocking.customquery.programmatic.dto.ProgrammaticMovieSummary;
import org.springframework.data.aerospike.examples.blocking.customquery.programmatic.entity.ProgrammaticCustomQueryMovieDocument;
import org.springframework.data.aerospike.examples.blocking.customquery.programmatic.repository.ProgrammaticCustomQueryMovieRepository;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;
import static org.springframework.data.aerospike.examples.support.ExampleCollections.toSortedList;

@Component
public class ProgrammaticCustomQueryExample {

    private final ProgrammaticCustomQueryMovieRepository repository;

    // This repository uses the inherited findUsingQuery(...) methods instead of @Query annotations
    public ProgrammaticCustomQueryExample(ProgrammaticCustomQueryMovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        repository.saveAll(List.of(
            new ProgrammaticCustomQueryMovieDocument("blocking-programmatic-query-1", "Stalker", "science-fiction", 1979),
            new ProgrammaticCustomQueryMovieDocument("blocking-programmatic-query-2", "Solaris", "science-fiction", 1972),
            new ProgrammaticCustomQueryMovieDocument("blocking-programmatic-query-3", "High and Low", "crime", 1963),
            new ProgrammaticCustomQueryMovieDocument("blocking-programmatic-query-4", "Decision to Leave", "crime", 2022),
            new ProgrammaticCustomQueryMovieDocument("blocking-programmatic-query-5", "Network", "drama", 1976)
        ));

        Qualifier scienceFiction = Qualifier.builder()
            .setPath("genre")
            .setFilterOperation(FilterOperation.EQ)
            .setValue("science-fiction")
            .build();

        Qualifier releasedInLateSeventies = Qualifier.builder()
            .setPath("releaseYear")
            .setFilterOperation(FilterOperation.BETWEEN)
            .setValue(1975)
            .setSecondValue(1980)
            .build();

        // AND combines explicit Qualifier objects. Both bins are indexed by the example fixture
        Query lateSeventiesScienceFiction = new Query(Qualifier.and(scienceFiction, releasedInLateSeventies));

        List<ProgrammaticCustomQueryMovieDocument> matches =
            toSortedList(repository.findUsingQuery(lateSeventiesScienceFiction),
                Comparator.comparing(ProgrammaticCustomQueryMovieDocument::getId));
        require(matches.size() == 1, "Expected one movie from the programmatic query");
        require("Stalker".equals(matches.get(0).getTitle()), "Programmatic query title did not match");

        // The target-class overload maps matching records into a projection DTO
        List<ProgrammaticMovieSummary> summaries =
            toSortedList(repository.findUsingQuery(lateSeventiesScienceFiction, ProgrammaticMovieSummary.class),
                Comparator.comparing(ProgrammaticMovieSummary::getTitle));
        require(summaries.size() == 1, "Expected one projected programmatic query result");
        require("Stalker".equals(summaries.get(0).getTitle()), "Projected title did not match");
        require(summaries.get(0).getReleaseYear() == 1979, "Projected release year did not match");

        System.out.println("Ran a blocking programmatic repository query with Qualifier and DTO projection");
    }

}
