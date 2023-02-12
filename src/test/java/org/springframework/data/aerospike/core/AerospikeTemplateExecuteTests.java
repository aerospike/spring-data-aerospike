/*
 * Copyright 2019 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.core;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AerospikeTemplateExecuteTests extends BaseBlockingIntegrationTests {

    @Test
    public void shouldTranslateException() {
        Key key = new Key(template.getNamespace(), "shouldTranslateException", "shouldTranslateException");
        Bin bin = new Bin("bin_name", "bin_value");

        template.getAerospikeClient().add(null, key, bin);
        assertThatThrownBy(() ->template.execute(() -> {
            IAerospikeClient client = template.getAerospikeClient();
            WritePolicy writePolicy = new WritePolicy(client.getWritePolicyDefault());
            writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;

            client.add(writePolicy, key, bin);
            return true;
        })).isInstanceOf(DuplicateKeyException.class);
    }
}
