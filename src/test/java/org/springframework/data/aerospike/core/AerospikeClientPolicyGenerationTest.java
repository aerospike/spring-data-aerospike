package org.springframework.data.aerospike.core;

import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.GenerationPolicy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.data.aerospike.SampleClasses.VersionedClass;
import org.springframework.data.aerospike.config.TestConfig;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(
        classes = AerospikeClientPolicyGenerationTest.Config.class,
        properties = {
                "expirationProperty: 1",
        }
)
public class AerospikeClientPolicyGenerationTest {
    private final static String recordField = "test-record-name";
    private final static String updatedRecordField = "record updated name";

    @Autowired
    private AerospikeTemplate template;

    @Test
    public void shouldSuccessfullyUpdateVersionedRecord() {
        // given
        String recordId = "test-record-id";

        VersionedClass record = new VersionedClass(recordId, recordField);
        template.insert(record);

        VersionedClass createdRecord = template.findById(recordId, VersionedClass.class);
        createdRecord.setField(updatedRecordField);

        // when
        template.update(createdRecord);

        // then
        VersionedClass updatedRecord = template.findById(recordId, VersionedClass.class);
        assertThat(updatedRecord.getField()).isEqualTo(updatedRecordField);
    }

    @Test(expected = RecoverableDataAccessException.class)
    public void shouldFailOnUpdateRecordWithDifferentVersion() {
        // given
        String recordId = "test-record-id-2";

        VersionedClass record = new VersionedClass(recordId, recordField);
        template.insert(record);

        VersionedClass createdRecord = template.findById(recordId, VersionedClass.class);
        createdRecord.setField(updatedRecordField);
        createdRecord.setVersion(100);

        // when
        template.update(createdRecord);
    }

    @Test
    public void shouldSuccessfullyDeleteRecordByIdWithSameGeneration() {
        // given
        String recordId = "test-record-id-3";

        VersionedClass record = new VersionedClass(recordId, recordField);
        template.insert(record);

        // and
        assertThat(template.findById(recordId, VersionedClass.class)).isNotNull();

        // when
        assertThat(template.delete(recordId, VersionedClass.class)).isTrue();

        // then
        assertThat(template.findById(recordId, VersionedClass.class)).isNull();
    }

    @Test
    public void shouldSuccessfullyDeleteRecordWithSameGeneration() {
        // given
        String recordId = "test-record-id-4";

        template.insert(new VersionedClass(recordId, recordField));

        // and
        VersionedClass createdUser = template.findById(recordId, VersionedClass.class);
        assertThat(createdUser).isNotNull();
        assertThat(createdUser.getVersion()).isEqualTo(1);

        // and
        createdUser.setField(updatedRecordField);
        template.save(createdUser);

        assertThat(template.findById(recordId, VersionedClass.class).getField()).isEqualTo(updatedRecordField);

        // when
        assertThat(template.delete(createdUser)).isTrue();

        // then
        assertThat(template.findById(recordId, VersionedClass.class)).isNull();
    }

    @Configuration
    @EnableAutoConfiguration
    static class Config extends TestConfig {

        @Override
        protected ClientPolicy getClientPolicy() {
            ClientPolicy policy = super.getClientPolicy();
            // should update / remove record with the same generations
            policy.writePolicyDefault.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
            return policy;
        }
    }
}
