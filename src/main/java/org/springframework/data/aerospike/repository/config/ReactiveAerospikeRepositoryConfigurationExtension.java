package org.springframework.data.aerospike.repository.config;

import org.springframework.data.repository.config.RepositoryConfigurationExtension;
import org.springframework.data.repository.core.RepositoryMetadata;

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
    protected boolean useRepositoryConfiguration(RepositoryMetadata metadata) {
        return metadata.isReactiveRepository();
    }
}
