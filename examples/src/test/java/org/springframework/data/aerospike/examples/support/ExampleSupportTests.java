package org.springframework.data.aerospike.examples.support;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExampleSupportTests {

    @Test
    void argsParseSelectionAndConnectionOptions() {
        Args args = Args.parse(new String[]{
            "blocking-crud,projection",
            "--hosts", "127.0.0.1:3000",
            "--namespace=dev",
            "--allow-non-test-namespace",
            "--fail-fast"
        });

        assertThat(args.listOnly()).isFalse();
        assertThat(args.examples()).containsExactly("blocking-crud", "projection");
        assertThat(args.hosts()).isEqualTo("127.0.0.1:3000");
        assertThat(args.namespace()).isEqualTo("dev");
        assertThat(args.allowNonTestNamespace()).isTrue();
        assertThat(args.failFast()).isTrue();
        assertThat(args.springProperties())
            .containsEntry("spring.aerospike.hosts", "127.0.0.1:3000")
            .containsEntry("spring.data.aerospike.namespace", "dev");
    }

    @Test
    void noArgsDefaultToListingExamples() {
        Args args = Args.parse(new String[0]);

        assertThat(args.listOnly()).isTrue();
        assertThat(args.examples()).isEmpty();
    }

    @Test
    void registryNamesAreUniqueAndOrdered() {
        List<String> names = ExampleRegistry.all().stream()
            .map(ExampleDefinition::name)
            .toList();

        assertThat(names)
            .doesNotHaveDuplicates()
            .containsExactly(
                "blocking-crud",
                "reactive-crud",
                "indexed-query",
                "projection",
                "indexed-annotation",
                "custom-query-dsl"
            );
    }

    @Test
    void runnerRejectsUnknownExampleNamesBeforeOpeningContext() {
        ExampleRunner runner = new ExampleRunner(ExampleRegistry.all());

        assertThatThrownBy(() -> runner.definitionsToRun(Args.parse(new String[]{"missing-example"})))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unknown example: missing-example");
    }

    @Test
    void runnerSkipsNonTestNamespaceWithoutExplicitOverride() {
        ExampleRunner runner = new ExampleRunner(ExampleRegistry.all());

        List<ExampleResult> results = runner.run(Args.parse(new String[]{"blocking-crud", "--namespace", "prod"}));

        assertThat(results).singleElement().satisfies(result -> {
            assertThat(result.status()).isEqualTo(ExampleStatus.SKIPPED);
            assertThat(result.message()).contains("--allow-non-test-namespace");
        });
    }
}
