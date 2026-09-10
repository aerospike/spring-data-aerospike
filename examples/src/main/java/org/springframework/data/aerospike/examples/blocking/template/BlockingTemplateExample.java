package org.springframework.data.aerospike.examples.blocking.template;

import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.core.WritePolicyBuilder;
import org.springframework.data.aerospike.examples.blocking.template.dto.TemplateMovieSummary;
import org.springframework.data.aerospike.examples.blocking.template.entity.TemplateMovieDocument;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;
import static org.springframework.data.aerospike.examples.support.ExampleCollections.toSortedList;

@Component
public class BlockingTemplateExample {

    private final AerospikeTemplate template;

    public BlockingTemplateExample(AerospikeTemplate template) {
        this.template = template;
    }

    public void run() {
        template.insertAll(seedMovies());

        // Batch reads by id are useful when the caller already knows the keys to load
        List<TemplateMovieDocument> firstTwoMovies = toSortedList(template.findByIds(
            List.of("blocking-template-1", "blocking-template-2"), TemplateMovieDocument.class),
            Comparator.comparing(TemplateMovieDocument::getId));
        require(firstTwoMovies.size() == 2, "Expected two movies from the batch id read");

        Query scienceFiction = matchingGenre("science-fiction");
        List<TemplateMovieDocument> queryMatches =
            toSortedList(template.find(scienceFiction, TemplateMovieDocument.class).toList(),
                Comparator.comparing(TemplateMovieDocument::getId));
        require(queryMatches.size() == 3, "Expected three science-fiction movies");
        require(template.exists(scienceFiction, TemplateMovieDocument.class),
            "Expected the indexed query to find at least one movie");
        require(template.count(releasedBetween(1990, 2000), TemplateMovieDocument.class) == 3,
            "Expected three 1990s movies from the indexed count query");

        List<TemplateMovieSummary> summaries = template
            .find(scienceFiction, TemplateMovieDocument.class, TemplateMovieSummary.class)
            .sorted(Comparator.comparingInt(TemplateMovieSummary::getReleaseYear))
            .toList();
        require("Dark City".equals(summaries.get(0).getTitle()), "First projected title did not match");
        require(summaries.get(0).getReleaseYear() == 1998, "First projected release year did not match");

        mutateSingleRecord();
        updateSelectedFieldOnly();
        persistWithCustomWritePolicy();

        template.deleteByIds(List.of("blocking-template-1", "blocking-template-2"), TemplateMovieDocument.class);
        require(!template.exists("blocking-template-1", TemplateMovieDocument.class),
            "Batch delete should remove the first movie");

        System.out.println("Ran blocking AerospikeTemplate query, batch, mutation, update, and persist operations");
    }

    private List<TemplateMovieDocument> seedMovies() {
        return List.of(
            new TemplateMovieDocument("blocking-template-1", "Dark City", "science-fiction", 1998, 5, 20),
            new TemplateMovieDocument("blocking-template-2", "The Matrix", "science-fiction", 1999, 5, 30),
            new TemplateMovieDocument("blocking-template-3", "Heat", "crime", 1995, 5, 40),
            new TemplateMovieDocument("blocking-template-4", "Arrival", "science-fiction", 2016, 5, 50)
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
        TemplateMovieDocument movie =
            new TemplateMovieDocument("blocking-template-mutation", "trix", "science-fiction", 1999, 5, 10);
        template.insert(movie);

        TemplateMovieDocument viewed = template.add(movie, "views", 5);
        require(viewed.getViews() == 15, "Atomic add should increment the views bin");

        TemplateMovieDocument prefixed = template.prepend(viewed, "title", "The Ma");
        TemplateMovieDocument renamed = template.append(prefixed, "title", " Reloaded");
        require("The Matrix Reloaded".equals(renamed.getTitle()), "Append/prepend should update the title bin");
    }

    private void updateSelectedFieldOnly() {
        TemplateMovieDocument movie =
            new TemplateMovieDocument("blocking-template-partial", "Primer", "science-fiction", 2004, 4, 100);
        template.insert(movie);

        template.update(new TemplateMovieDocument("blocking-template-partial", null, null, 0, 5, 0),
            List.of("rating"));

        TemplateMovieDocument updated = template.findById("blocking-template-partial", TemplateMovieDocument.class);
        require("Primer".equals(updated.getTitle()), "Partial update should leave the title unchanged");
        require(updated.getRating() == 5, "Partial update should change only the selected rating field");
    }

    private void persistWithCustomWritePolicy() {
        TemplateMovieDocument createdOnly =
            new TemplateMovieDocument("blocking-template-policy", "Moon", "science-fiction", 2009, 5, 60);
        WritePolicy createOnly = WritePolicyBuilder.builder(template.getAerospikeClient().getWritePolicyDefault())
            .recordExistsAction(RecordExistsAction.CREATE_ONLY)
            .build();

        template.persist(createdOnly, createOnly);
        require(template.exists("blocking-template-policy", TemplateMovieDocument.class),
            "Custom WritePolicy persist should create the record");
    }

}
