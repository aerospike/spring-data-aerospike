package org.springframework.data.aerospike.repository.config;

import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.aerospike.repository.ReactiveAerospikeRepository;
import org.springframework.data.aerospike.repository.support.ReactiveAerospikeRepositoryFactoryBean;
import org.springframework.data.repository.config.RepositoryConfigurationExtension;
import org.springframework.data.repository.core.RepositoryMetadata;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Collections;

/**
 * {@link RepositoryConfigurationExtension} for reactive Aerospike.
 *
 * @author Igor Ermolenko
 */
public class ReactiveAerospikeRepositoryConfigurationExtension extends BaseAerospikeRepositoryConfigurationExtension {

    @Override
    public String getModuleName() {
        return "Reactive Aerospike";
    }

    @Override
    protected String getModulePrefix() {
        return "reactive-aerospike";
    }

    @Override
    protected String getDefaultKeyValueTemplateRef() {
        return "reactiveAerospikeTemplate";
    }

    @Override
    public String getRepositoryFactoryBeanClassName() {
        return ReactiveAerospikeRepositoryFactoryBean.class.getName();
    }

    @Override
    protected Collection<Class<? extends Annotation>> getIdentifyingAnnotations() {
        return Collections.singleton(Document.class);
    }

    @Override
    protected Collection<Class<?>> getIdentifyingTypes() {
        return Collections.singleton(ReactiveAerospikeRepository.class);
    }

    @Override
    protected boolean useRepositoryConfiguration(RepositoryMetadata metadata) {
        return metadata.isReactiveRepository();
    }
}
