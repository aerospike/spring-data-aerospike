package org.springframework.data.aerospike.assertions;

import com.aerospike.client.Key;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.springframework.data.aerospike.config.AerospikeDataSettings;

public class KeyAssert extends AbstractAssert<KeyAssert, Key> {

    public KeyAssert(Key key) {
        super(key, KeyAssert.class);
    }

    public static KeyAssert assertThat(Key key) {
        return new KeyAssert(key);
    }

    @SuppressWarnings("UnusedReturnValue")
    public KeyAssert consistsOf(AerospikeDataSettings aerospikeDataSettings, String namespace, String setName,
                                Object expectedUserKey) {
        Assertions.assertThat(actual.namespace).isEqualTo(namespace);
        Assertions.assertThat(actual.setName).isEqualTo(setName);

        if (aerospikeDataSettings != null && aerospikeDataSettings.isKeepOriginalKeyTypes()) {
            Assertions.assertThat(verifyActualUserKeyType(expectedUserKey)).isTrue();
        } else {
            // String type is used for unsupported Aerospike key types and previously for all key types in older
            // versions of Spring Data Aerospike
            Assertions.assertThat(checkIfActualUserKeyTypeIsString()).isTrue();
        }
        return this;
    }

    private boolean verifyActualUserKeyType(Object expectedUserKey) {
        if (expectedUserKey.getClass() == Integer.class) {
            return actual.userKey.getObject() instanceof Long;
        } else { // String, Long and byte[] can be compared directly
            return actual.userKey.getObject().getClass().equals(expectedUserKey.getClass());
        }
    }

    private boolean checkIfActualUserKeyTypeIsString() {
        return actual.userKey.getObject() instanceof String;
    }
}
