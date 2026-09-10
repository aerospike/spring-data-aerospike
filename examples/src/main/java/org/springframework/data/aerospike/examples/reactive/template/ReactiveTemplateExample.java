package org.springframework.data.aerospike.examples.reactive.template;

import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.examples.reactive.template.dto.ReactiveTemplateMovieSummary;
import org.springframework.data.aerospike.examples.reactive.template.entity.ReactiveTemplateMovieDocument;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;
import static org.springframework.data.aerospike.examples.support.ExampleCollections.toSortedList;

@Component
public class ReactiveTemplateExample {

    private final ReactiveAerospikeTemplate template;

    public ReactiveTemplateExample(ReactiveAerospikeTemplate template) {
        this.template = template;
    }

    public void run() {
        template.insertAll(seedMovies()).collectList().block();

        // A runnable example blocks at scenario boundaries so each step is easy to follow
        List<ReactiveTemplateMovieDocument> firstTwoMovies = toSortedList(template
            .findByIds(List.of("reactive-template-1", "reactive-template-2"), ReactiveTemplateMovieDocument.class)
            .collectList()
            .block(), Comparator.comparing(ReactiveTemplateMovieDocument::getId),
            "Expected a reactive template movie list");
        require(firstTwoMovies.size() == 2, "Expected two movies from the reactive batch id read");

        Query scienceFiction = matchingGenre("science-fiction");
        List<ReactiveTemplateMovieDocument> queryMatches = toSortedList(template
            .find(scienceFiction, ReactiveTemplateMovieDocument.class)
            .collectList()
            .block(), Comparator.comparing(ReactiveTemplateMovieDocument::getId),
            "Expected a reactive template movie list");
        require(queryMatches.size() == 3, "Expected three reactive science-fiction movies");
        require(Boolean.TRUE.equals(template.exists(scienceFiction, ReactiveTemplateMovieDocument.class)
                .block()),
            "Expected the indexed reactive query to find at least one movie");
        require(template.count(releasedBetween(1990, 2000), ReactiveTemplateMovieDocument.class).block() == 3,
            "Expected three 1990s movies from the indexed reactive count query");

        List<ReactiveTemplateMovieSummary> summaries = toSortedList(template
            .find(scienceFiction, ReactiveTemplateMovieDocument.class, ReactiveTemplateMovieSummary.class)
            .collectList()
            .block(), Comparator.comparingInt(ReactiveTemplateMovieSummary::getReleaseYear),
            "Expected a reactive template summary list");
        require("Dark City".equals(summaries.get(0).getTitle()), "First reactive projected title did not match");
        require(summaries.get(0).getReleaseYear() == 1998, "First reactive projected release year did not match");

        mutateSingleRecord();
        updateSelectedFieldOnly();

        template.deleteByIds(List.of("reactive-template-1", "reactive-template-2"), ReactiveTemplateMovieDocument.class)
            .block();
        require(!Boolean.TRUE.equals(template.exists("reactive-template-1", ReactiveTemplateMovieDocument.class)
                .block()),
            "Reactive batch delete should remove the first movie");

        System.out.println("Ran reactive AerospikeTemplate query, batch, mutation, and update operations");
    }

    private List<ReactiveTemplateMovieDocument> seedMovies() {
        return List.of(
            new ReactiveTemplateMovieDocument("reactive-template-1", "Dark City", "science-fiction", 1998, 5, 20),
            new ReactiveTemplateMovieDocument("reactive-template-2", "The Matrix", "science-fiction", 1999, 5, 30),
            new ReactiveTemplateMovieDocument("reactive-template-3", "Heat", "crime", 1995, 5, 40),
            new ReactiveTemplateMovieDocument("reactive-template-4", "Arrival", "science-fiction", 2016, 5, 50)
        );
    }

    private Query matchingGenre(String genre) {
        return new Query(Qualifier.builder()
            .setPath("genre")
            .setFilterOperation(FilterOperation.EQ)
            .setValue(genre)
            .build());
    }

    private Query releasedBetween(int fromInclusive, int toInclusive) {
        return new Query(Qualifier.builder()
            .setPath("releaseYear")
            .setFilterOperation(FilterOperation.BETWEEN)
            .setValue(fromInclusive)
            .setSecondValue(toInclusive)
            .build());
    }

    private void mutateSingleRecord() {
        ReactiveTemplateMovieDocument movie =
            new ReactiveTemplateMovieDocument("reactive-template-mutation", "trix", "science-fiction", 1999, 5, 10);
        template.insert(movie).block();

        ReactiveTemplateMovieDocument viewed = template.add(movie, "views", 5).block();
        require(viewed.getViews() == 15, "Reactive atomic add should increment the views bin");

        ReactiveTemplateMovieDocument prefixed = template.prepend(viewed, "title", "The Ma").block();
        ReactiveTemplateMovieDocument renamed = template.append(prefixed, "title", " Reloaded").block();
        require("The Matrix Reloaded".equals(renamed.getTitle()),
            "Reactive append/prepend should update the title bin");
    }

    private void updateSelectedFieldOnly() {
        ReactiveTemplateMovieDocument movie =
            new ReactiveTemplateMovieDocument("reactive-template-partial", "Primer", "science-fiction", 2004, 4, 100);
        template.insert(movie).block();

        template.update(new ReactiveTemplateMovieDocument("reactive-template-partial", null, null, 0, 5, 0),
            List.of("rating")).block();

        ReactiveTemplateMovieDocument updated = template
            .findById("reactive-template-partial", ReactiveTemplateMovieDocument.class)
            .block();
        require("Primer".equals(updated.getTitle()), "Reactive partial update should leave the title unchanged");
        require(updated.getRating() == 5, "Reactive partial update should change only the selected rating field");
    }

}
