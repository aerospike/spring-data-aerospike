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

import java.util.Comparator;
import java.util.List;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;
import static org.springframework.data.aerospike.examples.support.ExampleCollections.toSortedList;

// Demonstrates blocking AerospikeTemplate operations for queries, projection, mutation, and deletes.
public class BlockingTemplateExample {

    private final AerospikeTemplate template;

    public BlockingTemplateExample(AerospikeTemplate template) {
        this.template = template;
    }

    public void run() {
        template.insertAll(seedMovies());

        saveSingleMovie();
        readSeveralMoviesById();
        Query scienceFiction = queryCountAndExists();
        projectQueryResults(scienceFiction);
        mutateSingleRecord();
        updateSelectedFieldOnly();
        persistWithCustomWritePolicy();
        deleteSeveralMoviesById();

        System.out.println("Ran blocking AerospikeTemplate query, batch, mutation, update, and persist operations");
    }

    private void saveSingleMovie() {
        TemplateMovieDocument movie =
            new TemplateMovieDocument("blocking-template-save", "Thief", "crime", 1981, 5, 15);

        // tag::template-save[]
        // save(...) writes a mapped entity directly through AerospikeTemplate.
        template.save(movie);

        // findById(...) reads the same record by its mapped @Id value.
        TemplateMovieDocument loaded = template.findById(movie.getId(), TemplateMovieDocument.class);
        // end::template-save[]

        require(loaded != null, "Saved template movie should be found");
        require("Thief".equals(loaded.getTitle()), "Saved template movie title did not match");
    }

    private void readSeveralMoviesById() {
        // tag::template-find-by-ids[]
        // Batch reads by id are useful when the caller already knows the keys to load
        List<TemplateMovieDocument> firstTwoMovies = toSortedList(template.findByIds(
            List.of("blocking-template-1", "blocking-template-2"), TemplateMovieDocument.class),
            Comparator.comparing(TemplateMovieDocument::getId));
        // end::template-find-by-ids[]

        require(firstTwoMovies.size() == 2, "Expected two movies from the batch id read");
    }

    private Query queryCountAndExists() {
        // tag::template-query-count-exists[]
        Query scienceFiction = matchingGenre("science-fiction");
        // find(...), exists(...), and count(...) can reuse the same Query shape.
        List<TemplateMovieDocument> queryMatches =
            toSortedList(template.find(scienceFiction, TemplateMovieDocument.class).toList(),
                Comparator.comparing(TemplateMovieDocument::getId));
        boolean scienceFictionExists = template.exists(scienceFiction, TemplateMovieDocument.class);
        long ninetiesMovieCount = template.count(releasedBetween(1990, 2000), TemplateMovieDocument.class);
        // end::template-query-count-exists[]

        require(queryMatches.size() == 3, "Expected three science-fiction movies");
        require(scienceFictionExists, "Expected the indexed query to find at least one movie");
        require(ninetiesMovieCount == 3, "Expected three 1990s movies from the indexed count query");
        return scienceFiction;
    }

    private void projectQueryResults(Query scienceFiction) {
        // tag::template-projection[]
        // The target-class overload maps matching records into a projection DTO.
        List<TemplateMovieSummary> summaries = template
            .find(scienceFiction, TemplateMovieDocument.class, TemplateMovieSummary.class)
            .sorted(Comparator.comparingInt(TemplateMovieSummary::getReleaseYear))
            .toList();
        // end::template-projection[]

        require("Dark City".equals(summaries.get(0).getTitle()), "First projected title did not match");
        require(summaries.get(0).getReleaseYear() == 1998, "First projected release year did not match");
    }

    private void deleteSeveralMoviesById() {
        // tag::template-delete-by-ids[]
        // deleteByIds(...) removes a known batch without first loading the entities.
        template.deleteByIds(List.of("blocking-template-1", "blocking-template-2"), TemplateMovieDocument.class);
        // end::template-delete-by-ids[]

        require(!template.exists("blocking-template-1", TemplateMovieDocument.class),
            "Batch delete should remove the first movie");
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
        // tag::template-bin-mutations[]
        TemplateMovieDocument movie =
            new TemplateMovieDocument("blocking-template-mutation", "trix", "science-fiction", 1999, 5, 10);
        template.insert(movie);

        // add(...) performs an atomic numeric bin mutation on the server.
        TemplateMovieDocument viewed = template.add(movie, "views", 5);

        // prepend(...) and append(...) mutate string bins without replacing the whole entity.
        TemplateMovieDocument prefixed = template.prepend(viewed, "title", "The Ma");
        TemplateMovieDocument renamed = template.append(prefixed, "title", " Reloaded");
        // end::template-bin-mutations[]

        require(viewed.getViews() == 15, "Atomic add should increment the views bin");
        require("The Matrix Reloaded".equals(renamed.getTitle()), "Append/prepend should update the title bin");
    }

    private void updateSelectedFieldOnly() {
        // tag::template-partial-update[]
        TemplateMovieDocument movie =
            new TemplateMovieDocument("blocking-template-partial", "Primer", "science-fiction", 2004, 4, 100);
        template.insert(movie);

        // update(..., fields) writes only the selected mapped property.
        template.update(new TemplateMovieDocument("blocking-template-partial", null, null, 0, 5, 0),
            List.of("rating"));

        TemplateMovieDocument updated = template.findById("blocking-template-partial", TemplateMovieDocument.class);
        // end::template-partial-update[]

        require("Primer".equals(updated.getTitle()), "Partial update should leave the title unchanged");
        require(updated.getRating() == 5, "Partial update should change only the selected rating field");
    }

    private void persistWithCustomWritePolicy() {
        // tag::template-custom-write-policy[]
        TemplateMovieDocument createdOnly =
            new TemplateMovieDocument("blocking-template-policy", "Moon", "science-fiction", 2009, 5, 60);
        WritePolicy createOnly = WritePolicyBuilder.builder(template.getAerospikeClient().getWritePolicyDefault())
            .recordExistsAction(RecordExistsAction.CREATE_ONLY)
            .build();

        // persist(..., WritePolicy) lets one operation override the template default policy.
        template.persist(createdOnly, createOnly);
        // end::template-custom-write-policy[]

        require(template.exists("blocking-template-policy", TemplateMovieDocument.class),
            "Custom WritePolicy persist should create the record");
    }

}
