package org.springframework.data.aerospike.query;

import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.query.cache.IndexesCache;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.query.model.IndexedField;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class QueryContextBuilderTests {

    @Test
    void buildKeepsUnchosenAndChildAsQualifierWhenMultipleIndexedFiltersHaveUnknownCardinality() {
        IndexesCache indexesCache = indexesCacheWithUnknownCardinality();

        Qualifier genreEquals = Qualifier.builder()
            .setPath("genre")
            .setFilterOperation(FilterOperation.EQ)
            .setValue("science-fiction")
            .build();
        Qualifier releaseYearBetween = Qualifier.builder()
            .setPath("releaseYear")
            .setFilterOperation(FilterOperation.BETWEEN)
            .setValue(2014)
            .setSecondValue(2015)
            .build();

        QueryContext context = new QueryContextBuilder(indexesCache)
            .build("test", "movies", new Query(Qualifier.and(genreEquals, releaseYearBetween)));

        assertThat(context.statement().getFilter()).isNotNull();
        assertThat(context.statement().getFilter().getName()).isEqualTo("genre");
        assertThat(context.qualifier()).isSameAs(releaseYearBetween);
    }

    @Test
    void buildReturnsNoQualifierWhenSingleAndChildIsHandledBySecondaryIndexFilter() {
        IndexesCache indexesCache = indexesCacheWithUnknownCardinality();

        Qualifier genreEquals = Qualifier.builder()
            .setPath("genre")
            .setFilterOperation(FilterOperation.EQ)
            .setValue("science-fiction")
            .build();

        QueryContext context = new QueryContextBuilder(indexesCache)
            .build("test", "movies", new Query(Qualifier.and(genreEquals)));

        assertThat(context.statement().getFilter()).isNotNull();
        assertThat(context.statement().getFilter().getName()).isEqualTo("genre");
        assertThat(context.qualifier()).isNull();
    }

    private IndexesCache indexesCacheWithUnknownCardinality() {
        IndexesCache indexesCache = mock(IndexesCache.class);
        when(indexesCache.hasIndexFor(any(IndexedField.class))).thenReturn(true);
        when(indexesCache.getAllIndexesForField(any(IndexedField.class)))
            .thenReturn(List.of(indexWithUnknownCardinality()));
        return indexesCache;
    }

    private Index indexWithUnknownCardinality() {
        return new Index("movie_idx", "test", "movies", "genre", IndexType.STRING, null);
    }
}
