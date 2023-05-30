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
        ).isZero();
        assertThat(indexesCache.hasIndexFor(new IndexedField(namespace, template.getSetName(TestPerson.class),
            "friend"))).isFalse();
    }

    @Test
    void usingIndexedAnnotationWithComplexCtxSingleQuotes() {
        class TestPerson {

            @Id
            String id;
            @Indexed(type = IndexType.STRING, name = "test_person_friend_address_keys_index", bin = "friend",
                collectionType = IndexCollectionType.MAPKEYS, ctx = "ab.cd.'10'.{#5}.{='1'}.[-1].[#100].[=20]")
            // CTX.mapKey(Value.get("ab")), CTX.mapKey(Value.get("cd")), CTX.mapKey(Value.get("10")), CTX.mapRank(5),
            // CTX.mapValue(Value.get("1")), CTX.listIndex(-1), CTX.listRank(100), CTX.listValue(Value.get(20))
            Address address;
        }
        indexRefresher.refreshIndexes();

        assertThat(
            additionalAerospikeTestOperations.getIndexes(template.getSetName(TestPerson.class)).stream()
                .filter(index -> index.getName()
                    .equals("test_person_friend_address_keys_index")
                    &&
                    CTX.toBase64(index.getCTX()).equals(CTX.toBase64(new CTX[]{CTX.mapKey(Value.get("ab")),
                        CTX.mapKey(Value.get("cd")), CTX.mapKey(Value.get("10")), CTX.mapRank(5),
                        CTX.mapValue(Value.get("1")), CTX.listIndex(-1), CTX.listRank(100),
                        CTX.listValue(Value.get(20))}))
                )
                .count()
        ).isEqualTo(1L);
        assertThat(indexesCache.hasIndexFor(new IndexedField(namespace, template.getSetName(TestPerson.class),
            "friend"))).isTrue();

        additionalAerospikeTestOperations.dropIndexIfExists(IndexedPerson.class,
            "test_person_friend_address_keys_index");
        indexRefresher.refreshIndexes();
    }

    @Test
    void usingIndexedAnnotationWithCtxDoubleQuotes() {
        class TestPerson {

            @Id
            String id;
            @Indexed(type = IndexType.STRING, name = "test_person_friend_address_keys_index", bin = "friend",
                collectionType = IndexCollectionType.MAPKEYS, ctx = "\"10\".{=\"1\"}")
            // CTX.mapKey(Value.get("10")), CTX.mapValue(Value.get("1"))
            Address address;
        }
        indexRefresher.refreshIndexes();

        assertThat(
            additionalAerospikeTestOperations.getIndexes(template.getSetName(TestPerson.class)).stream()
                .filter(index -> index.getName()
                    .equals("test_person_friend_address_keys_index")
                    &&
                    CTX.toBase64(index.getCTX()).equals(CTX.toBase64(new CTX[]{CTX.mapKey(Value.get("10")),
                        CTX.mapValue(Value.get("1"))}))
                )
                .count()
        ).isEqualTo(1L);
        assertThat(indexesCache.hasIndexFor(new IndexedField(namespace, template.getSetName(TestPerson.class),
            "friend"))).isTrue();

        additionalAerospikeTestOperations.dropIndexIfExists(IndexedPerson.class,
            "test_person_friend_address_keys_index");
        indexRefresher.refreshIndexes();
    }

    @Test
    void usingIndexedAnnotationWithTooManyDots() {
        class TestPerson {

            @Id
            String id;
            @Indexed(type = IndexType.STRING, name = "test_person_many_dots_index", bin = "friend",
                collectionType = IndexCollectionType.MAPKEYS, ctx = "ab....cd..")
            Address address;
        }
        indexRefresher.refreshIndexes();

        assertThatThrownBy(() -> additionalAerospikeTestOperations.getIndexes(template.getSetName(TestPerson.class)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("@Indexed annotation 'ab....cd..' contains empty context");
    }

    @Test
    void usingIndexedAnnotationTooSmallContextLength() {
        class TestPerson {

            @Id
            String id;
            @Indexed(type = IndexType.STRING, name = "test_person_too_small_context_length_index", bin = "friend",
                collectionType = IndexCollectionType.MAPKEYS, ctx = "ab.[]")
            Address address;
        }
        indexRefresher.refreshIndexes();

        assertThatThrownBy(() -> additionalAerospikeTestOperations.getIndexes(template.getSetName(TestPerson.class)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("@Indexed annotation: context string '[]' has no content");
    }

    @Test
    void usingIndexedAnnotationWrongClosingBracket() {
        class TestPerson {

            @Id
            String id;
            @Indexed(type = IndexType.STRING, name = "test_person_wrong_closing_bracket_index", bin = "friend",
                collectionType = IndexCollectionType.MAPKEYS, ctx = "ab.{cd]")
            Address address;
        }
        indexRefresher.refreshIndexes();

        assertThatThrownBy(() -> additionalAerospikeTestOperations.getIndexes(template.getSetName(TestPerson.class)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("@Indexed annotation: brackets mismatch, expecting '}', got ']' instead");
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
