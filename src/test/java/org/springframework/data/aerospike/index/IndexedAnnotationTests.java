package org.springframework.data.aerospike.index;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.annotation.Indexed;
import org.springframework.data.aerospike.query.model.IndexedField;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.IndexedPerson;
import org.springframework.data.annotation.Id;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class IndexedAnnotationTests extends BaseBlockingIntegrationTests {

    @Test
    void usingIndexedAnnotationWithCtx() {
        class TestPerson {

            @Id
            String id;
            @Indexed(type = IndexType.STRING, name = "test_person_friend_address_keys_index", bin = "friend",
                collectionType = IndexCollectionType.MAPKEYS, ctx = "address") // CTX.mapKey(Value.get("address"))
            Address address;
        }
        indexRefresher.refreshIndexes();

        assertThat(
            additionalAerospikeTestOperations.getIndexes(template.getSetName(TestPerson.class)).stream()
                .filter(index -> index.getName()
                    .equals("test_person_friend_address_keys_index")
                    &&
                    CTX.toBase64(index.getCTX()).equals(CTX.toBase64(new CTX[]{CTX.mapKey(Value.get("address"))}))
                )
                .count()
        ).isEqualTo(1L);
        assertThat(indexesCache.hasIndexFor(new IndexedField(namespace, template.getSetName(TestPerson.class),
            "friend"))).isTrue();

        additionalAerospikeTestOperations.dropIndexIfExists(IndexedPerson.class,
            "test_person_friend_address_keys_index");
        indexRefresher.refreshIndexes();

        assertThat(
            additionalAerospikeTestOperations.getIndexes(template.getSetName(TestPerson.class)).stream()
                .filter(index -> index.getName()
                    .equals("test_person_friend_address_keys_index")
                    &&
                    CTX.toBase64(index.getCTX()).equals(CTX.toBase64(new CTX[]{CTX.mapKey(Value.get("address"))}))
                )
                .count()
        ).isEqualTo(0L);
        assertThat(indexesCache.hasIndexFor(new IndexedField(namespace, template.getSetName(TestPerson.class),
            "friend"))).isFalse();
    }

    @Test
    void usingIndexedAnnotationWithComplexCtx() {
        class TestPerson {

            @Id
            String id;
            @Indexed(type = IndexType.STRING, name = "test_person_friend_address_keys_index", bin = "friend",
                collectionType = IndexCollectionType.MAPKEYS, ctx = "ab.cd.[-1].\"10\".")
            // CTX.mapKey(Value.get("ab")), CTX.mapKey(Value.get("cd")), CTX.listIndex(-1), CTX.mapKey(Value.get("9"))
            Address address;
        }
        indexRefresher.refreshIndexes();

        assertThat(
            additionalAerospikeTestOperations.getIndexes(template.getSetName(TestPerson.class)).stream()
                .filter(index -> index.getName()
                    .equals("test_person_friend_address_keys_index")
                    &&
                    CTX.toBase64(index.getCTX()).equals(CTX.toBase64(new CTX[]{CTX.mapKey(Value.get("ab")),
                        CTX.mapKey(Value.get("cd")), CTX.listIndex(-1), CTX.mapKey(Value.get("10"))}))
                )
                .count()
        ).isEqualTo(1L);
        assertThat(indexesCache.hasIndexFor(new IndexedField(namespace, template.getSetName(TestPerson.class),
            "friend"))).isTrue();

    }

    @Test
    void usingIndexedAnnotationWithIncorrectMapRank() {
        class TestPerson {

            @Id
            String id;
            @Indexed(type = IndexType.STRING, name = "test_person_friend_address_keys_index", bin = "friend",
                collectionType = IndexCollectionType.MAPKEYS, ctx = "{#address}") // rank must be integer
            Address address;
        }
        indexRefresher.refreshIndexes();

        assertThatThrownBy(() -> additionalAerospikeTestOperations.getIndexes(template.getSetName(TestPerson.class)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("@Indexed annotation map rank: expecting only integer values, got 'address' instead");
    }

    @Test
    void usingIndexedAnnotationWithIncorrectListRank() {
        class TestPerson {

            @Id
            String id;
            @Indexed(type = IndexType.STRING, name = "test_person_friend_address_keys_index", bin = "friend",
                collectionType = IndexCollectionType.MAPKEYS, ctx = "[#address]") // rank must be integer
            Address address;
        }
        indexRefresher.refreshIndexes();

        assertThatThrownBy(() -> additionalAerospikeTestOperations.getIndexes(template.getSetName(TestPerson.class)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("@Indexed annotation list rank: expecting only integer values, got 'address' instead");
    }

    @Test
    void usingIndexedAnnotationWithIncorrectMapIndex() {
        class TestPerson {

            @Id
            String id;
            @Indexed(type = IndexType.STRING, name = "test_person_friend_address_keys_index", bin = "friend",
                collectionType = IndexCollectionType.MAPKEYS, ctx = "{address}") // index must be integer
            Address address;
        }
        indexRefresher.refreshIndexes();

        assertThatThrownBy(() -> additionalAerospikeTestOperations.getIndexes(template.getSetName(TestPerson.class)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("@Indexed annotation map index: expecting only integer values, got 'address' instead");
    }

    @Test
    void usingIndexedAnnotationWithIncorrectListIndex() {
        class TestPerson {

            @Id
            String id;
            @Indexed(type = IndexType.STRING, name = "test_person_friend_address_keys_index", bin = "friend",
                collectionType = IndexCollectionType.MAPKEYS, ctx = "[address]") // index must be integer
            Address address;
        }
        indexRefresher.refreshIndexes();

        assertThatThrownBy(() -> additionalAerospikeTestOperations.getIndexes(template.getSetName(TestPerson.class)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("@Indexed annotation list index: expecting only integer values, got 'address' instead");
    }
}
