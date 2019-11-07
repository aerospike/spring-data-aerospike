package org.springframework.data.aerospike.repository.config;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.data.aerospike.repository.query.AerospikeQueryCreator;
import org.springframework.data.keyvalue.repository.config.KeyValueRepositoryConfigurationExtension;
import org.springframework.data.keyvalue.repository.config.QueryCreatorType;
import org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource;

import java.util.Map;

/**
 * @author Oliver Gierke
 * @author Igor Ermolenko
 */
public abstract class BaseAerospikeRepositoryConfigurationExtension extends KeyValueRepositoryConfigurationExtension {

    @Override
    public void postProcess(BeanDefinitionBuilder builder, AnnotationRepositoryConfigurationSource config) {

        AnnotationAttributes attributes = config.getAttributes();

        builder.addPropertyReference("operations", attributes.getString(KEY_VALUE_TEMPLATE_BEAN_REF_ATTRIBUTE));
        builder.addPropertyValue("queryCreator", getQueryCreatorType(config));
        builder.addPropertyReference("mappingContext", MAPPING_CONTEXT_BEAN_NAME);
    }

    /**
     * Detects the query creator type to be used for the factory to set. Will lookup a {@link QueryCreatorType} annotation
     * on the {@code @Enable}-annotation or use {@link SpelQueryCreator} if not found.
     *
     * @param config
     * @return
     */
    private static Class<?> getQueryCreatorType(AnnotationRepositoryConfigurationSource config) {

        AnnotationMetadata metadata = config.getEnableAnnotationMetadata();

        Map<String, Object> queryCreatorFoo = metadata.getAnnotationAttributes(QueryCreatorType.class.getName());

        if (queryCreatorFoo == null) {
            return AerospikeQueryCreator.class;
        }

        AnnotationAttributes queryCreatorAttributes = new AnnotationAttributes(queryCreatorFoo);
        return queryCreatorAttributes.getClass("value");
    }
}
