package org.springframework.data.aerospike.examples.support;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class Args {

    private static final String DEFAULT_HOSTS = "localhost:3000";
    private static final String DEFAULT_NAMESPACE = "test";

    private final boolean listOnly;
    private final List<String> examples;
    private final String hosts;
    private final String namespace;
    private final boolean allowNonTestNamespace;
    private final boolean failFast;

    private Args(boolean listOnly, List<String> examples, String hosts, String namespace,
                 boolean allowNonTestNamespace, boolean failFast) {
        this.listOnly = listOnly;
        this.examples = List.copyOf(examples);
        this.hosts = hosts;
        this.namespace = namespace;
        this.allowNonTestNamespace = allowNonTestNamespace;
        this.failFast = failFast;
    }

    public static Args parse(String[] rawArgs) {
        boolean listOnly = rawArgs.length == 0;
        List<String> examples = new ArrayList<>();
        String hosts = DEFAULT_HOSTS;
        String namespace = DEFAULT_NAMESPACE;
        boolean allowNonTestNamespace = false;
        boolean failFast = false;

        for (int i = 0; i < rawArgs.length; i++) {
            String arg = rawArgs[i];

            if ("list".equals(arg) || "--list".equals(arg)) {
                listOnly = true;
            } else if ("all".equals(arg)) {
                examples.add("all");
            } else if ("--hosts".equals(arg)) {
                hosts = requireValue(rawArgs, ++i, "--hosts");
            } else if (arg.startsWith("--hosts=")) {
                hosts = requireInlineValue(arg, "--hosts=");
            } else if ("--namespace".equals(arg)) {
                namespace = requireValue(rawArgs, ++i, "--namespace");
            } else if (arg.startsWith("--namespace=")) {
                namespace = requireInlineValue(arg, "--namespace=");
            } else if ("--allow-non-test-namespace".equals(arg)) {
                allowNonTestNamespace = true;
            } else if ("--fail-fast".equals(arg)) {
                failFast = true;
            } else if (arg.startsWith("--")) {
                throw new IllegalArgumentException("Unknown option: " + arg);
            } else {
                for (String example : arg.split(",")) {
                    if (!example.isBlank()) {
                        examples.add(example.trim());
                    }
                }
            }
        }

        return new Args(listOnly, examples, hosts, namespace, allowNonTestNamespace, failFast);
    }

    private static String requireValue(String[] args, int index, String optionName) {
        if (index >= args.length || args[index].startsWith("--")) {
            throw new IllegalArgumentException(optionName + " requires a value");
        }
        return args[index];
    }

    private static String requireInlineValue(String arg, String optionPrefix) {
        String value = arg.substring(optionPrefix.length());
        if (value.isBlank()) {
            throw new IllegalArgumentException(optionPrefix.substring(0, optionPrefix.length() - 1) + " requires a value");
        }
        return value;
    }

    public boolean listOnly() {
        return listOnly;
    }

    public List<String> examples() {
        return examples;
    }

    public boolean runsAllExamples() {
        return examples.contains("all");
    }

    public String hosts() {
        return hosts;
    }

    public String namespace() {
        return namespace;
    }

    public boolean allowNonTestNamespace() {
        return allowNonTestNamespace;
    }

    public boolean failFast() {
        return failFast;
    }

    public Map<String, Object> springProperties() {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("spring.aerospike.hosts", hosts);
        properties.put("spring.data.aerospike.namespace", namespace);
        properties.put("spring.data.aerospike.scans-enabled", "false");
        properties.put("spring.data.aerospike.index-cache-refresh-seconds", "0");
        properties.put("spring.data.aerospike.server-version-refresh-seconds", "0");
        return properties;
    }

    public static String usage() {
        return """
            Usage:
              ./examples/run_examples list
              ./examples/run_examples all --hosts localhost:3000 --namespace test
              ./examples/run_examples blocking-crud --hosts localhost:3000 --namespace test

            Options:
              --hosts <host:port[,host:port]>       Aerospike hosts, defaults to localhost:3000
              --namespace <namespace>               Aerospike namespace, defaults to test
              --allow-non-test-namespace            Allow cleanup in a namespace other than test
              --fail-fast                           Stop after the first failed example
            """;
    }
}
