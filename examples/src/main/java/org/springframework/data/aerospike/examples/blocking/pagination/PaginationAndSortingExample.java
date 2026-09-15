package org.springframework.data.aerospike.examples.blocking.pagination;

import org.springframework.data.aerospike.examples.blocking.pagination.entity.PaginationMovieDocument;
import org.springframework.data.aerospike.examples.blocking.pagination.repository.PaginationMovieRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;

import java.util.List;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;

// Demonstrates repository sorting, Page, Slice, and offset pagination queries.
public class PaginationAndSortingExample {

    private final PaginationMovieRepository repository;

    public PaginationAndSortingExample(PaginationMovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        repository.saveAll(seedMovies());

        standaloneSorting();
        pageQuery();
        sliceQuery();
        sortedOffsetPage();
        pureIdPaginationWithoutSort();

        System.out.println("Ran repository pagination and sorting examples");
    }

    private void standaloneSorting() {
        // tag::pagination-standalone-sorting[]
        // Sort can be passed by itself when no page metadata is needed.
        List<PaginationMovieDocument> scienceFiction = repository.findByGenre(
            "science-fiction", Sort.by("releaseYear").ascending());
        // end::pagination-standalone-sorting[]

        require(scienceFiction.size() == 4, "Expected four science-fiction movies");
        require("Dark City".equals(scienceFiction.get(0).getTitle()), "Expected earliest movie first");
        require("Arrival".equals(scienceFiction.get(3).getTitle()), "Expected latest movie last");
    }

    private void pageQuery() {
        // tag::pagination-page[]
        // Page requests include content and total-count metadata.
        Page<PaginationMovieDocument> page = repository.findByReleaseYearLessThan(
            2010, PageRequest.of(0, 2, Sort.by("releaseYear")));
        // end::pagination-page[]

        require(page.hasContent(), "Expected page content");
        require(page.getNumberOfElements() == 2, "Expected two movies on the first page");
        require(page.getTotalElements() == 4, "Expected four movies released before 2010");
    }

    private void sliceQuery() {
        // tag::pagination-slice[]
        // Slice requests fetch one extra row to report whether another slice exists.
        Slice<PaginationMovieDocument> slice = repository.findByReleaseYearGreaterThan(
            1990, PageRequest.of(0, 2, Sort.by("releaseYear")));
        // end::pagination-slice[]

        require(slice.hasContent(), "Expected slice content");
        require(slice.hasNext(), "Expected another slice after the first two movies");
    }

    private void sortedOffsetPage() {
        // tag::pagination-sorted-offset[]
        // Sorted offset pagination applies the Sort before selecting the requested page window.
        Page<PaginationMovieDocument> secondPage = repository.findByReleaseYearLessThan(
            2020, PageRequest.of(1, 2, Sort.by("releaseYear")));
        // end::pagination-sorted-offset[]

        require(secondPage.getNumber() == 1, "Expected the second page");
        require(secondPage.getNumberOfElements() == 2, "Expected two movies on the second page");
    }

    private void pureIdPaginationWithoutSort() {
        // tag::pagination-pure-id-unsorted-offset[]
        // ID pagination preserves the caller-provided id order when no Sort is supplied.
        List<String> movieIds = seedMovies().stream()
            .map(PaginationMovieDocument::getId)
            .toList();
        Page<PaginationMovieDocument> secondIdPage = repository.findAllById(movieIds, PageRequest.of(1, 2));
        // end::pagination-pure-id-unsorted-offset[]

        require(secondIdPage.getNumber() == 1, "Expected the second ID page");
        require(secondIdPage.getNumberOfElements() == 2, "Expected two movies on the second ID page");
        require("pagination-3".equals(secondIdPage.getContent().get(0).getId()),
            "Expected ID pagination to preserve source ID order");
        require("pagination-4".equals(secondIdPage.getContent().get(1).getId()),
            "Expected ID pagination to preserve source ID order");
    }

    private List<PaginationMovieDocument> seedMovies() {
        return List.of(
            new PaginationMovieDocument("pagination-1", "Dark City", "science-fiction", 1998),
            new PaginationMovieDocument("pagination-2", "The Matrix", "science-fiction", 1999),
            new PaginationMovieDocument("pagination-3", "Heat", "crime", 1995),
            new PaginationMovieDocument("pagination-4", "Moon", "science-fiction", 2009),
            new PaginationMovieDocument("pagination-5", "Arrival", "science-fiction", 2016)
        );
    }
}
