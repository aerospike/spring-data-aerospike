package org.springframework.data.aerospike.examples.support;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ExampleRunner {

    private final List<ExampleDefinition> definitions;

    public ExampleRunner(List<ExampleDefinition> definitions) {
        this.definitions = List.copyOf(definitions);
    }

    public List<ExampleDefinition> definitionsToRun(Args args) {
        if (args.runsAllExamples()) {
            if (args.examples().size() > 1) {
                throw new IllegalArgumentException("'all' cannot be combined with named examples.");
            }
            return definitions;
        }

        Map<String, ExampleDefinition> byName = new LinkedHashMap<>();
        definitions.forEach(definition -> byName.put(definition.name(), definition));

        List<ExampleDefinition> selected = new ArrayList<>();
        for (String example : args.examples()) {
            ExampleDefinition definition = byName.get(example);
            if (definition == null) {
                throw new IllegalArgumentException("Unknown example: " + example);
            }
            selected.add(definition);
        }

        if (selected.isEmpty()) {
            throw new IllegalArgumentException("No examples selected. Use 'list' or provide an example name.");
        }

        return selected;
    }

    public List<ExampleResult> run(Args args) {
        List<ExampleResult> results = new ArrayList<>();

        for (ExampleDefinition definition : definitionsToRun(args)) {
            ExampleResult result = runOne(definition, args);
            results.add(result);

            if (args.failFast() && result.status() == ExampleStatus.FAILED) {
                break;
            }
        }

        return results;
    }

    private ExampleResult runOne(ExampleDefinition definition, Args args) {
        if (!"test".equals(args.namespace()) && !args.allowNonTestNamespace()) {
            return ExampleResult.skipped(definition.name(), "Namespace '" + args.namespace()
                + "' requires --allow-non-test-namespace before examples can clean up sda_examples_* resources.");
        }

        Instant started = Instant.now();
        AnnotationConfigApplicationContext context = null;
        boolean beforeContextRefreshStarted = false;
        boolean contextRefreshed = false;
        ExampleResult result;

        try {
            beforeContextRefreshStarted = true;
            definition.fixture().beforeContextRefresh(args);
            context = new AnnotationConfigApplicationContext();
            context.getEnvironment().getPropertySources()
                .addFirst(new MapPropertySource("exampleCliArguments", args.springProperties()));
            context.register(definition.configurationClass());
            context.refresh();
            contextRefreshed = true;

            definition.fixture().setup(context);
            invokeRunMethod(context.getBean(definition.exampleClass()));
            definition.fixture().verify(context);

            result = ExampleResult.passed(definition.name(), Duration.between(started, Instant.now()), "completed");
        } catch (ExampleSkippedException skipped) {
            result = ExampleResult.skipped(definition.name(), Duration.between(started, Instant.now()),
                skipped.getMessage());
        } catch (Throwable failure) {
            result = ExampleResult.failed(definition.name(), Duration.between(started, Instant.now()),
                unwrap(failure));
        }

        RuntimeException cleanupFailure = cleanup(definition, args, context, beforeContextRefreshStarted,
            contextRefreshed);
        return resultWithCleanupFailure(definition, result, cleanupFailure, started);
    }

    private RuntimeException cleanup(ExampleDefinition definition, Args args, AnnotationConfigApplicationContext context,
                                     boolean beforeContextRefreshStarted, boolean contextRefreshed) {
        RuntimeException cleanupFailure = null;
        try {
            if (context != null && context.isActive()) {
                definition.fixture().cleanup(context);
            } else if (beforeContextRefreshStarted && !contextRefreshed) {
                definition.fixture().cleanupAfterContextRefreshFailure(args);
            }
        } catch (RuntimeException failure) {
            cleanupFailure = failure;
        } finally {
            cleanupFailure = closeContext(context, cleanupFailure);
        }
        return cleanupFailure;
    }

    private RuntimeException closeContext(AnnotationConfigApplicationContext context,
                                          RuntimeException cleanupFailure) {
        if (context == null) {
            return cleanupFailure;
        }

        try {
            context.close();
            return cleanupFailure;
        } catch (RuntimeException failure) {
            return combineCleanupFailures(cleanupFailure, failure);
        }
    }

    private ExampleResult resultWithCleanupFailure(ExampleDefinition definition, ExampleResult result,
                                                  RuntimeException cleanupFailure, Instant started) {
        if (cleanupFailure == null) {
            return result;
        }

        if (result.cause() != null) {
            result.cause().addSuppressed(cleanupFailure);
            return result;
        }

        return ExampleResult.failed(definition.name(), Duration.between(started, Instant.now()), cleanupFailure);
    }

    private RuntimeException combineCleanupFailures(RuntimeException primary, RuntimeException secondary) {
        if (primary == null) {
            return secondary;
        }
        primary.addSuppressed(secondary);
        return primary;
    }

    private void invokeRunMethod(Object exampleBean) throws Throwable {
        Method runMethod = ReflectionUtils.findMethod(exampleBean.getClass(), "run");
        if (runMethod == null) {
            throw new IllegalStateException("Example bean " + exampleBean.getClass().getName()
                + " must declare a public run() method");
        }

        try {
            runMethod.invoke(exampleBean);
        } catch (InvocationTargetException ex) {
            throw ex.getTargetException();
        }
    }

    private Throwable unwrap(Throwable failure) {
        if (failure instanceof InvocationTargetException ex && ex.getTargetException() != null) {
            return ex.getTargetException();
        }
        return failure;
    }
}
