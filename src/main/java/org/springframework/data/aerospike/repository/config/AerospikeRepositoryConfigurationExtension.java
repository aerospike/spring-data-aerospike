package org.springframework.data.aerospike.repository.config;

import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.aerospike.repository.AerospikeRepository;
import org.springframework.data.aerospike.repository.support.AerospikeRepositoryFactoryBean;
import org.springframework.data.repository.config.RepositoryConfigurationExtension;
import org.springframework.data.repository.core.RepositoryMetadata;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Collections;

/**
 * {@link RepositoryConfigurationExtension} for Aerospike.
 *
 * @author Oliver Gierke
 */
public class AerospikeRepositoryConfigurationExtension extends BaseAerospikeRepositoryConfigurationExtension {

    @Override
    public String getModuleName() {
        return "Aerospike";
    }

    @Override
    protected String getModulePrefix() {
        return "aerospike";
    }

    @Override
    protected String getDefaultKeyValueTemplateRef() {
        return "aerospikeTemplate";
    }

    @Override
    public String getRepositoryFactoryBeanClassName() {
        return AerospikeRepositoryFactoryBean.class.getName();
    }

    @Override
    protected Collection<Class<? extends Annotation>> getIdentifyingAnnotations() {
        return Collections.singleton(Document.class);
    }

    @Override
    protected Collection<Class<?>> getIdentifyingTypes() {
        return Collections.singleton(AerospikeRepository.class);
    }

    @Override
    protected boolean useRepositoryConfiguration(RepositoryMetadata metadata) {
        return !metadata.isReactiveRepository();
    }
}
