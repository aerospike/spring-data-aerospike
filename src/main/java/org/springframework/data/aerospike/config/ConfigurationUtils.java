package org.springframework.data.aerospike.config;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

import static org.springframework.data.aerospike.config.AerospikeDataConfigurationSupport.CONFIG_PREFIX_DATA;

public class ConfigurationUtils {

    private static final String CONFIG_PROPERTY_PROXY_CLIENT = CONFIG_PREFIX_DATA + ".use-proxy-client";

    static class ClientProxyPropertyFalse implements Condition {
        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            String useClientProxy = context.getEnvironment().getProperty(CONFIG_PROPERTY_PROXY_CLIENT);
            return useClientProxy == null || useClientProxy.equalsIgnoreCase("false");
        }
    }

    static class ClientProxyPropertyTrue implements Condition {
        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            String useClientProxy = context.getEnvironment().getProperty(CONFIG_PROPERTY_PROXY_CLIENT);
            return useClientProxy != null && useClientProxy.equalsIgnoreCase("true");
        }
    }
}
