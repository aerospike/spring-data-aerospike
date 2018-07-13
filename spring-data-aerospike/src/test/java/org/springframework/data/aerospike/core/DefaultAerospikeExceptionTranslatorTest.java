package org.springframework.data.aerospike.core;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import org.junit.Test;
import org.springframework.dao.*;
import org.springframework.data.keyvalue.core.UncategorizedKeyValueException;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultAerospikeExceptionTranslatorTest {

    private DefaultAerospikeExceptionTranslator translator = new DefaultAerospikeExceptionTranslator();

    @Test
    public void shouldTranslateKeyExistError() {
        AerospikeException cause = new AerospikeException(ResultCode.KEY_EXISTS_ERROR);
        DataAccessException actual = translator.translateExceptionIfPossible(cause);
        assertThat(actual).isExactlyInstanceOf(DuplicateKeyException.class);
    }

    @Test
    public void shouldTranslateKeyNonFoundError() {
        AerospikeException cause = new AerospikeException(ResultCode.KEY_NOT_FOUND_ERROR);
        DataAccessException actual = translator.translateExceptionIfPossible(cause);
        assertThat(actual).isExactlyInstanceOf(DataRetrievalFailureException.class);
    }

    @Test
    public void shouldTranslateTimeoutError() {
        AerospikeException cause = new AerospikeException(ResultCode.TIMEOUT);
        DataAccessException actual = translator.translateExceptionIfPossible(cause);
        assertThat(actual).isExactlyInstanceOf(QueryTimeoutException.class);
    }

    @Test
    public void shouldTranslateQueryTimeoutError() {
        AerospikeException cause = new AerospikeException(ResultCode.QUERY_TIMEOUT);
        DataAccessException actual = translator.translateExceptionIfPossible(cause);
        assertThat(actual).isExactlyInstanceOf(QueryTimeoutException.class);
    }

    @Test
    public void shouldTranslateQueryDeviceOverloadError() {
        AerospikeException cause = new AerospikeException(ResultCode.DEVICE_OVERLOAD);
        DataAccessException actual = translator.translateExceptionIfPossible(cause);
        assertThat(actual).isExactlyInstanceOf(TransientDataAccessResourceException.class);
    }

    @Test
    public void shouldTranslateQueryNoMoreConnectionsError() {
        AerospikeException cause = new AerospikeException(ResultCode.NO_MORE_CONNECTIONS);
        DataAccessException actual = translator.translateExceptionIfPossible(cause);
        assertThat(actual).isExactlyInstanceOf(TransientDataAccessResourceException.class);
    }

    @Test
    public void shouldTranslateQueryKeyBusyError() {
        AerospikeException cause = new AerospikeException(ResultCode.KEY_BUSY);
        DataAccessException actual = translator.translateExceptionIfPossible(cause);
        assertThat(actual).isExactlyInstanceOf(TransientDataAccessResourceException.class);
    }

    @Test
    public void shouldTranslateAerospikeError() {
        AerospikeException cause = new AerospikeException();
        DataAccessException actual = translator.translateExceptionIfPossible(cause);
        assertThat(actual).isExactlyInstanceOf(RecoverableDataAccessException.class);
    }

    @Test
    public void shouldTranslateUnknownError() {
        RuntimeException cause = new RuntimeException();
        DataAccessException actual = translator.translateExceptionIfPossible(cause);
        assertThat(actual).isExactlyInstanceOf(UncategorizedKeyValueException.class);
    }
}
