package org.springframework.data.aerospike.examples.support;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;

import java.util.Arrays;
import java.util.List;

public interface ExampleFixture {

    default void setup(ConfigurableApplicationContext context) {
    }

    default void verify(ConfigurableApplicationContext context) {
    }

    default void cleanup(ConfigurableApplicationContext context) {
    }

    static ExampleFixture none() {
        return new ExampleFixture() {
        };
    }

    static ExampleFixture cleanSet(Class<?> entityClass) {
        return new CleanupFixture(entityClass, List.of(), false);
    }

    static ExampleFixture cleanSetAndIndexes(Class<?> entityClass, String... indexNames) {
        return new CleanupFixture(entityClass, Arrays.asList(indexNames), true);
    }

    static ExampleFixture cleanSetThenDropIndexesOnCleanup(Class<?> entityClass, String... indexNames) {
        return new CleanupFixture(entityClass, Arrays.asList(indexNames), false);
    }

    class CleanupFixture implements ExampleFixture {

        private final Class<?> entityClass;
        private final List<String> indexNames;
        private final boolean dropIndexesInSetup;

        CleanupFixture(Class<?> entityClass, List<String> indexNames, boolean dropIndexesInSetup) {
            this.entityClass = entityClass;
            this.indexNames = indexNames;
            this.dropIndexesInSetup = dropIndexesInSetup;
        }

        @Override
        public void setup(ConfigurableApplicationContext context) {
            cleanupRecords(context);
            if (dropIndexesInSetup) {
                cleanupIndexes(context);
            }
        }

        @Override
        public void cleanup(ConfigurableApplicationContext context) {
            cleanupRecords(context);
            cleanupIndexes(context);
        }

        private void cleanupRecords(ConfigurableApplicationContext context) {
            AerospikeTemplate blockingTemplate = context.getBeanProvider(AerospikeTemplate.class).getIfAvailable();
            if (blockingTemplate != null) {
                blockingTemplate.deleteAll(entityClass);
                return;
            }

            ReactiveAerospikeTemplate reactiveTemplate =
                context.getBeanProvider(ReactiveAerospikeTemplate.class).getIfAvailable();
            if (reactiveTemplate != null) {
                reactiveTemplate.deleteAll(entityClass).block();
            }
        }

        private void cleanupIndexes(ConfigurableApplicationContext context) {
            AerospikeTemplate blockingTemplate = context.getBeanProvider(AerospikeTemplate.class).getIfAvailable();
            if (blockingTemplate != null) {
                indexNames.forEach(indexName -> dropBlockingIndex(blockingTemplate, indexName));
                return;
            }

            ReactiveAerospikeTemplate reactiveTemplate =
                context.getBeanProvider(ReactiveAerospikeTemplate.class).getIfAvailable();
            if (reactiveTemplate != null) {
                indexNames.forEach(indexName -> dropReactiveIndex(reactiveTemplate, indexName));
            }
        }

        private void dropBlockingIndex(AerospikeTemplate template, String indexName) {
            try {
                template.deleteIndex(entityClass, indexName);
            } catch (RuntimeException ignored) {
                // Cleanup should not fail a scenario because a previous run already removed the index
            }
        }

        private void dropReactiveIndex(ReactiveAerospikeTemplate template, String indexName) {
            try {
                template.deleteIndex(entityClass, indexName).block();
            } catch (RuntimeException ignored) {
                // Cleanup should not fail a scenario because a previous run already removed the index
            }
        }
    }
}
