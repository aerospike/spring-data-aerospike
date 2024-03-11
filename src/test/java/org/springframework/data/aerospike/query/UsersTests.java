package org.springframework.data.aerospike.query;

import com.aerospike.client.Value;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.Query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.RECORD_COUNT;
import static org.springframework.data.aerospike.query.QueryEngineTestDataPopulator.USERS_SET;

class UsersTests extends BaseQueryEngineTests {

    @Test
    void allUsers() {
        KeyRecordIterator it = queryEngine.select(namespace, USERS_SET, null, null);

        assertThat(it).toIterable().hasSize(RECORD_COUNT);
    }

    @Test
    void usersInterrupted() {
        try (KeyRecordIterator it = queryEngine.select(namespace, USERS_SET, null, null)) {
            int counter = 0;
            while (it.hasNext()) {
                it.next();
                counter++;
                if (counter >= 1000)
                    break;
            }
        }
    }

    @Test
    void usersInNorthRegion() {
        Qualifier qualifier = Qualifier.builder()
            .setField("region")
            .setFilterOperation(FilterOperation.EQ)
            .setValue(Value.get("n"))
            .build();

        KeyRecordIterator it = queryEngine.select(namespace, USERS_SET, null, new Query(qualifier));

        assertThat(it).toIterable()
            .isNotEmpty()
            .allSatisfy(rec -> assertThat(rec.record.getString("region")).isEqualTo("n"));
    }
}
