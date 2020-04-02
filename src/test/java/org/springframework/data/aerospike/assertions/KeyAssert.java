package org.springframework.data.aerospike.assertions;

import com.aerospike.client.Key;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;

public class KeyAssert extends AbstractAssert<KeyAssert, Key> {

    public KeyAssert(Key key) {
        super(key, KeyAssert.class);
    }

    public static KeyAssert assertThat(Key key) {
        return new KeyAssert(key);
    }

    public KeyAssert consistsOf(String namespace, String setName, Object userKey) {
        Assertions.assertThat(actual.namespace).isEqualTo(namespace);
        Assertions.assertThat(actual.setName).isEqualTo(setName);
        Assertions.assertThat(actual.userKey.getObject()).isEqualTo(String.valueOf(userKey));
        return this;
    }
}
