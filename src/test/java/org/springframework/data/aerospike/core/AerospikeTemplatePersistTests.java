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

import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import org.junit.Test;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.aerospike.BaseIntegrationTests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.SampleClasses.CustomCollectionClass;

public class AerospikeTemplatePersistTests extends BaseIntegrationTests {

    @Test
    public void shouldPersistWithCustomWritePolicy() {
        CustomCollectionClass initial = new CustomCollectionClass(id, "data");

        WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;

        template.persist(initial, writePolicy);

        CustomCollectionClass actual = template.findById(id, CustomCollectionClass.class);
        assertThat(actual).isEqualTo(initial);
    }

    @Test(expected = DataRetrievalFailureException.class)
    public void shouldNotPersistWithCustomWritePolicy() {
        CustomCollectionClass initial = new CustomCollectionClass(id, "data");

        WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;

        template.persist(initial, writePolicy);
    }
}
