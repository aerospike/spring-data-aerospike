package org.springframework.data.aerospike.query.cache;

import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.query.model.IndexKey;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.IndexUtils;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class IndexTests extends BaseBlockingIntegrationTests {

	private static final String SET = "index-test";
	private static final String BIN_1 = "bin-1";
	private static final String BIN_2 = "bin-2";
	private static final String BIN_3 = "bin-3";
	private static final String INDEX_NAME = "index-1";
	private static final String INDEX_NAME_2 = "index-2";
	private static final String INDEX_NAME_3 = "index-3";

	@Override
	@BeforeEach
	public void setUp() {
		IndexUtils.dropIndex(client, namespace, SET, INDEX_NAME);
		IndexUtils.dropIndex(client, namespace, null, INDEX_NAME_2);
		IndexUtils.dropIndex(client, namespace, SET, INDEX_NAME_2);
		IndexUtils.dropIndex(client, namespace, SET, INDEX_NAME_3);
		indexRefresher.refreshIndexes();
	}

	@Test
	public void refreshIndexes_findsNewlyCreatedIndex() {
		Optional<Index> index = indexesCache.getIndex(new IndexKey(namespace, SET, BIN_1, IndexType.NUMERIC));
		assertThat(index).isEmpty();

		IndexUtils.createIndex(client, namespace, SET, INDEX_NAME, BIN_1, IndexType.NUMERIC);

		indexRefresher.refreshIndexes();

		index = indexesCache.getIndex(new IndexKey(namespace, SET, BIN_1, IndexType.NUMERIC));
		assertThat(index).isPresent()
				.hasValueSatisfying(value -> {
					assertThat(value.getName()).isEqualTo(INDEX_NAME);
					assertThat(value.getNamespace()).isEqualTo(namespace);
					assertThat(value.getSet()).isEqualTo(SET);
					assertThat(value.getBin()).isEqualTo(BIN_1);
					assertThat(value.getType()).isEqualTo(IndexType.NUMERIC);
				});
	}

	@Test
	public void refreshIndexes_removesDeletedIndex() {
		IndexUtils.createIndex(client, namespace, SET, INDEX_NAME, BIN_1, IndexType.NUMERIC);

		indexRefresher.refreshIndexes();

		assertThat(indexesCache.getIndex(new IndexKey(namespace, SET, BIN_1, IndexType.NUMERIC))).isPresent();

		IndexUtils.dropIndex(client, namespace, SET, INDEX_NAME);

		indexRefresher.refreshIndexes();

		assertThat(indexesCache.getIndex(new IndexKey(namespace, SET, BIN_1, IndexType.NUMERIC))).isEmpty();
	}

	@Test
	public void refreshIndexes_indexWithoutSetCanBeParsed() {
		IndexUtils.createIndex(client, namespace, null, INDEX_NAME_2, BIN_2, IndexType.STRING);

		indexRefresher.refreshIndexes();

		Optional<Index> index = indexesCache.getIndex(new IndexKey(namespace, null, BIN_2, IndexType.STRING));
		assertThat(index).isPresent()
				.hasValueSatisfying(value -> {
					assertThat(value.getName()).isEqualTo(INDEX_NAME_2);
					assertThat(value.getNamespace()).isEqualTo(namespace);
					assertThat(value.getSet()).isNull();
					assertThat(value.getBin()).isEqualTo(BIN_2);
					assertThat(value.getType()).isEqualTo(IndexType.STRING);
				});
	}

	@Test
	public void refreshIndexes_indexWithGeoTypeCanBeParsed() {
		IndexUtils.createIndex(client, namespace, SET, INDEX_NAME_3, BIN_3, IndexType.GEO2DSPHERE);

		indexRefresher.refreshIndexes();

		Optional<Index> index = indexesCache.getIndex(new IndexKey(namespace, SET, BIN_3, IndexType.GEO2DSPHERE));
		assertThat(index).isPresent()
				.hasValueSatisfying(value -> {
					assertThat(value.getName()).isEqualTo(INDEX_NAME_3);
					assertThat(value.getNamespace()).isEqualTo(namespace);
					assertThat(value.getSet()).isEqualTo(SET);
					assertThat(value.getBin()).isEqualTo(BIN_3);
					assertThat(value.getType()).isEqualTo(IndexType.GEO2DSPHERE);
				});
	}

	@Test
	public void refreshIndexes_multipleIndexesCanBeParsed() {
		IndexUtils.createIndex(client, namespace, SET, INDEX_NAME, BIN_1, IndexType.NUMERIC);
		IndexUtils.createIndex(client, namespace, null, INDEX_NAME_2, BIN_2, IndexType.STRING);
		IndexUtils.createIndex(client, namespace, SET, INDEX_NAME_3, BIN_3, IndexType.GEO2DSPHERE);

		indexRefresher.refreshIndexes();

		assertThat(indexesCache.getIndex(new IndexKey(namespace, SET, BIN_1, IndexType.NUMERIC))).isPresent();
		assertThat(indexesCache.getIndex(new IndexKey(namespace, null, BIN_2, IndexType.STRING))).isPresent();
		assertThat(indexesCache.getIndex(new IndexKey(namespace, SET, BIN_3, IndexType.GEO2DSPHERE))).isPresent();
		assertThat(indexesCache.getIndex(new IndexKey("unknown", null, "unknown", IndexType.NUMERIC))).isEmpty();
	}

	@Test
	public void refreshIndexes_indexesForTheSameBinCanBeParsed() {
		IndexUtils.createIndex(client, namespace, SET, INDEX_NAME, BIN_1, IndexType.NUMERIC);
		IndexUtils.createIndex(client, namespace, SET, INDEX_NAME_2, BIN_1, IndexType.STRING);

		indexRefresher.refreshIndexes();

		assertThat(indexesCache.getIndex(new IndexKey(namespace, SET, BIN_1, IndexType.NUMERIC))).hasValueSatisfying(value -> {
			assertThat(value.getName()).isEqualTo(INDEX_NAME);
			assertThat(value.getNamespace()).isEqualTo(namespace);
			assertThat(value.getSet()).isEqualTo(SET);
			assertThat(value.getBin()).isEqualTo(BIN_1);
			assertThat(value.getType()).isEqualTo(IndexType.NUMERIC);
		});
		assertThat(indexesCache.getIndex(new IndexKey(namespace, SET, BIN_1, IndexType.STRING))).hasValueSatisfying(value -> {
			assertThat(value.getName()).isEqualTo(INDEX_NAME_2);
			assertThat(value.getNamespace()).isEqualTo(namespace);
			assertThat(value.getSet()).isEqualTo(SET);
			assertThat(value.getBin()).isEqualTo(BIN_1);
			assertThat(value.getType()).isEqualTo(IndexType.STRING);
		});

	}

	@Test
	public void isIndexedBin_returnsTrueForIndexedField() {
		IndexUtils.createIndex(client, namespace, SET, INDEX_NAME, BIN_1, IndexType.NUMERIC);
		IndexUtils.createIndex(client, namespace, SET, INDEX_NAME_2, BIN_2, IndexType.NUMERIC);
		indexRefresher.refreshIndexes();

		assertThat(indexesCache.getIndex(new IndexKey(namespace, SET, BIN_1, IndexType.NUMERIC))).isPresent();
		assertThat(indexesCache.getIndex(new IndexKey(namespace, SET, BIN_2, IndexType.NUMERIC))).isPresent();
	}

	@Test
	public void isIndexedBin_returnsFalseForNonIndexedField() {
		assertThat(indexesCache.getIndex(new IndexKey(namespace, SET, BIN_2, IndexType.NUMERIC))).isEmpty();
		assertThat(indexesCache.getIndex(new IndexKey(namespace, SET, BIN_2, IndexType.STRING))).isEmpty();
		assertThat(indexesCache.getIndex(new IndexKey(namespace, SET, BIN_2, IndexType.GEO2DSPHERE))).isEmpty();
	}

	@Test
	public void getIndex_returnsEmptyForNonExistingIndex() {
		Optional<Index> index = indexesCache.getIndex(new IndexKey(namespace, SET, BIN_1, IndexType.NUMERIC));

		assertThat(index).isEmpty();
	}

}
