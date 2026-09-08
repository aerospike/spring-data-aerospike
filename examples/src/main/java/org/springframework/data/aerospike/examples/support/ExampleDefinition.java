package org.springframework.data.aerospike.examples.support;

import org.springframework.util.Assert;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public record ExampleDefinition(String name, String kind, Class<?> configurationClass, Class<?> exampleClass,
                                ExampleFixture fixture, Set<String> tags) {

    public ExampleDefinition {
        Assert.hasText(name, "Example name must not be empty");
        Assert.hasText(kind, "Example kind must not be empty");
        Assert.notNull(configurationClass, "Configuration class must not be null");
        Assert.notNull(exampleClass, "Example class must not be null");
        fixture = fixture == null ? ExampleFixture.none() : fixture;
        tags = tags == null ? Set.of() : Set.copyOf(new LinkedHashSet<>(tags));
    }

    public static ExampleDefinition of(String name, String kind, Class<?> configurationClass, Class<?> exampleClass,
                                       ExampleFixture fixture, String... tags) {
        return new ExampleDefinition(name, kind, configurationClass, exampleClass, fixture,
            new LinkedHashSet<>(List.of(tags)));
    }
}
