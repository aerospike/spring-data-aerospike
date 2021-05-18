package org.springframework.data.aerospike.core;

import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.core.model.GroupedEntities;
import org.springframework.data.aerospike.core.model.GroupedKeys;

public class AerospikeTemplateFindByEntitiesTests extends BaseBlockingIntegrationTests implements AbstractFindByEntitiesTest {

    @Override
    public <T> void save(T obj) {
        template.save(obj);
    }

    @Override
    public GroupedEntities findByIds(GroupedKeys groupedKeys) {
        return template.findByIds(groupedKeys);
    }
}
