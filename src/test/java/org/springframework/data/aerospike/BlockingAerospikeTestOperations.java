package org.springframework.data.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexType;
import lombok.RequiredArgsConstructor;
import org.awaitility.Awaitility;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.aerospike.core.AerospikeTemplate;

import java.time.Duration;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@RequiredArgsConstructor
public class BlockingAerospikeTestOperations {

    private final AerospikeTemplate template;
    private final AerospikeClient client;

    public void deleteAll(Class... entityClasses) {
        Arrays.asList(entityClasses).forEach(template::delete);
        Arrays.asList(entityClasses).forEach(this::awaitUntilSetIsEmpty);
    }

    @SuppressWarnings("unchecked")
    private void awaitUntilSetIsEmpty(Class entityClass) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .until(() -> isEmptySet(client, template.getNamespace(), entityClass));
    }

    public <T> boolean isEmptySet(IAerospikeClient client, String namespace, Class<T> entityClass) {
        String answer = Info.request(client.getNodes()[0], "sets/" + namespace + "/" + template.getSetName(entityClass));
        return answer.isEmpty()
                || Stream.of(answer.split(";")).allMatch(s -> s.contains("objects=0"));
    }

    public <T> void createIndexIfNotExists(Class<T> domainType, String indexName, String binName, IndexType indexType) {
        try {
            template.createIndex(domainType, indexName, binName, indexType);
        } catch (InvalidDataAccessResourceUsageException e) {
            // ignore: index already exists
        }
    }

    public void addNewFieldToSavedDataInAerospike(Key key) {
        Record initial = client.get(new Policy(), key);
        Bin[] bins = Stream.concat(
                initial.bins.entrySet().stream().map(e -> new Bin(e.getKey(), e.getValue())),
                Stream.of(new Bin("notPresent", "cats"))).toArray(Bin[]::new);
        WritePolicy policy = new WritePolicy();
        policy.recordExistsAction = RecordExistsAction.REPLACE;

        client.put(policy, key, bins);

        Record updated = client.get(new Policy(), key);
        assertThat(updated.bins.get("notPresent")).isEqualTo("cats");
    }

}
