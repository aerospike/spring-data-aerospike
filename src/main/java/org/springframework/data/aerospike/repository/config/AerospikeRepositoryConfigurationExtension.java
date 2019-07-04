package org.springframework.data.aerospike.repository.config;

import org.springframework.data.repository.config.RepositoryConfigurationExtension;
import org.springframework.data.repository.core.RepositoryMetadata;

/**
 * {@link RepositoryConfigurationExtension} for Aerospike.
 *
 * @author Oliver Gierke
 */
public class AerospikeRepositoryConfigurationExtension extends BaseAerospikeRepositoryConfigurationExtension {

    /*
     * (non-Javadoc)
     * @see org.springframework.data.keyvalue.repository.config.KeyValueRepositoryConfigurationExtension#getModuleName()
     */
    @Override
    public String getModuleName() {
        return "Aerospike";
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.keyvalue.repository.config.KeyValueRepositoryConfigurationExtension#getModulePrefix()
     */
    @Override
    protected String getModulePrefix() {
        return "aerospike";
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.keyvalue.repository.config.KeyValueRepositoryConfigurationExtension#getDefaultKeyValueTemplateRef()
     */
    @Override
    protected String getDefaultKeyValueTemplateRef() {
        return "aerospikeTemplate";
    }

    @Override
    protected boolean useRepositoryConfiguration(RepositoryMetadata metadata) {
        return !metadata.isReactiveRepository();
    }
}
