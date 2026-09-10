package org.springframework.data.aerospike.examples.support;

import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.aerospike.mapping.Document;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExampleSupportTests {

    private static final List<String> hookOrderEvents = new ArrayList<>();

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
                "custom-query-dsl",
                "blocking-query-methods",
                "reactive-query-methods",
                "blocking-custom-query-programmatic",
                "reactive-custom-query-programmatic",
                "blocking-template",
                "reactive-template",
                "blocking-custom-converters",
                "reactive-custom-converters",
                "blocking-transactions",
                "reactive-transactions"
            );
    }

    @Test
    void multiIndexFixtureFactoryStoresEveryDirectIndexDefinition() throws Exception {
        ExampleFixture fixture = ExampleFixture.cleanSetAndCreateIndexesBeforeContextRefresh(
            HookOrderDocument.class,
            ExampleFixture.index("sda_examples_test_genre_idx", "genre", IndexType.STRING),
            ExampleFixture.index("sda_examples_test_year_idx", "releaseYear", IndexType.NUMERIC)
        );

        List<String> indexNames = fixtureField(fixture, "indexNames");
        List<ExampleFixture.DirectIndexDefinition> indexesToCreate =
            fixtureField(fixture, "indexesToCreateBeforeContextRefresh");

        assertThat(indexNames)
            .containsExactly("sda_examples_test_genre_idx", "sda_examples_test_year_idx");
        assertThat(indexesToCreate)
            .extracting(ExampleFixture.DirectIndexDefinition::indexName)
            .containsExactly("sda_examples_test_genre_idx", "sda_examples_test_year_idx");
        assertThat(indexesToCreate)
            .extracting(ExampleFixture.DirectIndexDefinition::binName)
            .containsExactly("genre", "releaseYear");
        assertThat(indexesToCreate)
            .extracting(ExampleFixture.DirectIndexDefinition::indexType)
            .containsExactly(IndexType.STRING, IndexType.NUMERIC);
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

    @Test
    void runnerReportsExampleSkippedExceptionAndStillCleansUp() {
        hookOrderEvents.clear();
        ExampleFixture fixture = new ExampleFixture() {

            @Override
            public void setup(ConfigurableApplicationContext context) {
                hookOrderEvents.add("setup");
            }

            @Override
            public void verify(ConfigurableApplicationContext context) {
                hookOrderEvents.add("verify");
            }

            @Override
            public void cleanup(ConfigurableApplicationContext context) {
                hookOrderEvents.add("cleanup");
            }
        };
        ExampleRunner runner = new ExampleRunner(List.of(ExampleDefinition.of(
            "skip-contract",
            "test",
            SkippingConfiguration.class,
            SkippingExample.class,
            fixture
        )));

        List<ExampleResult> results = runner.run(Args.parse(new String[]{"skip-contract"}));

        assertThat(results).singleElement().satisfies(result -> {
            assertThat(result.status()).isEqualTo(ExampleStatus.SKIPPED);
            assertThat(result.message()).contains("Server 8.0.0+");
        });
        assertThat(hookOrderEvents).containsExactly("setup", "run", "cleanup");
    }

    @Test
    void runnerInvokesPreContextHookBeforeRefreshingContext() {
        hookOrderEvents.clear();
        ExampleFixture fixture = new ExampleFixture() {

            @Override
            public void beforeContextRefresh(Args args) {
                hookOrderEvents.add("beforeContextRefresh");
            }

            @Override
            public void setup(ConfigurableApplicationContext context) {
                hookOrderEvents.add("setup");
            }

            @Override
            public void verify(ConfigurableApplicationContext context) {
                hookOrderEvents.add("verify");
            }

            @Override
            public void cleanup(ConfigurableApplicationContext context) {
                hookOrderEvents.add("cleanup");
            }
        };
        ExampleRunner runner = new ExampleRunner(List.of(ExampleDefinition.of(
            "hook-order",
            "test",
            HookOrderConfiguration.class,
            HookOrderExample.class,
            fixture
        )));

        List<ExampleResult> results = runner.run(Args.parse(new String[]{"hook-order"}));

        assertThat(results).singleElement()
            .satisfies(result -> assertThat(result.status()).isEqualTo(ExampleStatus.PASSED));
        assertThat(hookOrderEvents)
            .containsExactly("beforeContextRefresh", "setup", "run", "verify", "cleanup");
    }

    @Configuration(proxyBeanMethods = false)
    static class HookOrderConfiguration {

        @Bean
        HookOrderExample hookOrderExample() {
            return new HookOrderExample();
        }
    }

    static class HookOrderExample {

        public void run() {
            hookOrderEvents.add("run");
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class SkippingConfiguration {

        @Bean
        SkippingExample skippingExample() {
            return new SkippingExample();
        }
    }

    static class SkippingExample {

        public void run() {
            hookOrderEvents.add("run");
            throw new ExampleSkippedException("Server 8.0.0+ is required");
        }
    }

    @Document(collection = "sda_examples_hook_order")
    static class HookOrderDocument {
    }

    @SuppressWarnings("unchecked")
    private static <T> T fixtureField(ExampleFixture fixture, String fieldName) throws Exception {
        Field field = fixture.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(fixture);
    }
}
