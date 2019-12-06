package org.springframework.data.aerospike.repository.config;

import org.springframework.data.repository.config.RepositoryBeanDefinitionRegistrarSupport;
import org.springframework.data.repository.config.RepositoryConfigurationExtension;

import java.lang.annotation.Annotation;

/**
 * Reactive aerospike {@link RepositoryBeanDefinitionRegistrarSupport} implementation.
 *
 * @author Igor Ermolenko
 */
public class ReactiveAerospikeRepositoriesRegistrar extends RepositoryBeanDefinitionRegistrarSupport {

    @Override
    protected Class<? extends Annotation> getAnnotation() {
        return EnableReactiveAerospikeRepositories.class;
    }

    @Override
    protected RepositoryConfigurationExtension getExtension() {
        return new ReactiveAerospikeRepositoryConfigurationExtension();
    }
}
