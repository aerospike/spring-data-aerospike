/**
 *
 */
package org.springframework.data.aerospike.convert;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.TestPropertySource;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.query.cache.IndexRefresher.INDEX_CACHE_REFRESH_SECONDS;

@TestPropertySource(properties = {INDEX_CACHE_REFRESH_SECONDS + " = 0", "createIndexesOnStartup = false"})
// this test class does not require secondary indexes created on startup
public class AerospikeReadDataTest {

    @Test
    public void shouldThrowExceptionIfRecordIsNull() {
        assertThatThrownBy(() -> AerospikeReadData.forRead(new Key("namespace", "set", 867), null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Record must not be null");
    }

    @Test
    public void shouldThrowExceptionIfRecordBinsIsNull() {
        assertThatThrownBy(() -> AerospikeReadData.forRead(new Key("namespace", "set", 867), new Record(null, 0, 0)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Record bins must not be null");
    }

    @Test
    public void shouldThrowExceptionIfKeyIsNull() {
        assertThatThrownBy(() -> AerospikeReadData.forRead(null, new Record(Collections.emptyMap(), 0, 0)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Key must not be null");
    }
}
