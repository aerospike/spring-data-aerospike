package org.springframework.data.aerospike.examples;

import org.springframework.data.aerospike.examples.support.Args;
import org.springframework.data.aerospike.examples.support.ExampleDefinition;
import org.springframework.data.aerospike.examples.support.ExampleRegistry;
import org.springframework.data.aerospike.examples.support.ExampleResult;
import org.springframework.data.aerospike.examples.support.ExampleRunner;
import org.springframework.data.aerospike.examples.support.ExampleStatus;

import java.util.List;

public final class Main {

    private Main() {
    }

    public static void main(String[] rawArgs) {
        try {
            Args args = Args.parse(rawArgs);
            List<ExampleDefinition> definitions = ExampleRegistry.all();

            if (args.listOnly()) {
                printExamples(definitions);
                return;
            }

            List<ExampleResult> results = new ExampleRunner(definitions).run(args);
            printResults(results);

            if (results.stream().anyMatch(result -> result.status() == ExampleStatus.FAILED)) {
                System.exit(1);
            }
            System.exit(0);
        } catch (IllegalArgumentException ex) {
            System.err.println(ex.getMessage());
            System.err.println();
            System.err.println(Args.usage());
            System.exit(2);
        }
    }

    private static void printExamples(List<ExampleDefinition> definitions) {
        System.out.println("Registered Spring Data Aerospike examples:");
        definitions.forEach(definition -> System.out.printf("  %-22s %-8s %s%n",
            definition.name(), definition.kind(), definition.tags()));
    }

    private static void printResults(List<ExampleResult> results) {
        results.forEach(result -> {
            System.out.printf("[%s] %s (%d ms) %s%n", result.status(), result.name(),
                result.duration().toMillis(), result.message());
            if (result.cause() != null) {
                result.cause().printStackTrace(System.err);
            }
        });

        long passed = count(results, ExampleStatus.PASSED);
        long skipped = count(results, ExampleStatus.SKIPPED);
        long failed = count(results, ExampleStatus.FAILED);
        System.out.printf("Summary: %d passed, %d skipped, %d failed%n", passed, skipped, failed);
    }

    private static long count(List<ExampleResult> results, ExampleStatus status) {
        return results.stream().filter(result -> result.status() == status).count();
    }
}
