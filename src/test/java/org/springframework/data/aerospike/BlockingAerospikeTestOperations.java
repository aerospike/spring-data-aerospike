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
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.Value;
import org.awaitility.Awaitility;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@RequiredArgsConstructor
public class BlockingAerospikeTestOperations {

	private final AerospikeTemplate template;
	private final AerospikeClient client;
	private final GenericContainer aerospike;

	public void assertScansForSet(String setName, Consumer<List<? extends ScanJob>> consumer) {
		List<ScanJob> jobs = getScans();
		List<ScanJob> jobsForSet = jobs.stream().filter(job -> job.set.equals(setName)).collect(Collectors.toList());
		assertThat(jobsForSet)
				.as("Scan jobs for set: " + setName)
				.satisfies(consumer);
	}

	public void assertNoScansForSet(String setName) {
		List<ScanJob> jobs = getScans();
		List<ScanJob> jobsForSet = jobs.stream().filter(job -> setName.equals(job.set)).collect(Collectors.toList());
		assertThat(jobsForSet)
				.as("Scan jobs for set: " + setName)
				.isEmpty();
	}

	@SneakyThrows
	public List<ScanJob> getScans() {
		Container.ExecResult execResult = aerospike.execInContainer("asinfo", "-v", "scan-list");
		String stdout = execResult.getStdout();
		return getScanJobs(stdout);
	}

	private List<ScanJob> getScanJobs(String stdout) {
		if (stdout.isEmpty() || stdout.equals("\n")) {
			return Collections.emptyList();
		}
		return Arrays.stream(stdout.replaceAll("\n", "").split(";"))
				.map(job -> {
					String[] pairs = job.split(":");
					Map<String, String> pairsMap = Arrays.stream(pairs)
							.map(pair -> {
								String[] kv = pair.split("=");
								return new AbstractMap.SimpleEntry<>(kv[0], kv[1]);
							})
							.collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
					return ScanJob.builder()
							.module(pairsMap.get("module"))
							.set(pairsMap.get("set"))
							.udfFunction(pairsMap.get("udf-function"))
							.status(pairsMap.get("status"))
							.build();
				}).collect(Collectors.toList());
	}

	@SneakyThrows
	public void killAllScans() {
		Container.ExecResult execResult = aerospike.execInContainer("asinfo", "-v", "scan-abort-all:");
		assertThat(execResult.getStdout())
				.as("Scan jobs killed")
				.contains("OK");
	}

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

	public <T> void createIndexIfNotExists(Class<T> entityClass, String indexName, String binName, IndexType indexType) {
		IndexUtils.createIndex(client, template.getNamespace(), template.getSetName(entityClass), indexName, binName, indexType);
	}

	public <T> void dropIndexIfExists(Class<T> entityClass, String indexName) {
		IndexUtils.dropIndex(client, template.getNamespace(), template.getSetName(entityClass), indexName);
	}

	// Do not use this code in production!
	// This will not guarantee the correct answer from Aerospike Server for all cases.
	// Also it requests index status only from one Aerospike node, which is OK for tests, and NOT OK for Production cluster.
	public boolean indexExists(String indexName) {
		return IndexUtils.indexExists(client, template.getNamespace(), indexName);
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

	@Value
	@Builder
	public static class ScanJob {

		@NonNull
		String module;
		String set;
		String udfFunction;
		@NonNull
		String status;
	}

}
