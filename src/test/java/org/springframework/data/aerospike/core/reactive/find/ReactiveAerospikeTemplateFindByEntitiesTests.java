package org.springframework.data.aerospike.core.reactive.find;

import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.core.blocking.find.AbstractFindByEntitiesTest;
import org.springframework.data.aerospike.core.model.GroupedEntities;
import org.springframework.data.aerospike.core.model.GroupedKeys;
import reactor.core.scheduler.Schedulers;

public class ReactiveAerospikeTemplateFindByEntitiesTests
    extends BaseReactiveIntegrationTests implements AbstractFindByEntitiesTest {

    @Override
    public <T> void save(T obj) {
        reactiveTemplate.save(obj)
            .subscribeOn(Schedulers.parallel())
            .block();
    }

    @Override
    public <T> void delete(T obj) {
        reactiveTemplate.delete(obj)
            .subscribeOn(Schedulers.parallel())
            .block();
    }

    @Override
    public GroupedEntities findByIds(GroupedKeys groupedKeys) {
        return reactiveTemplate.findByIds(groupedKeys)
            .subscribeOn(Schedulers.parallel())
            .block();
    }
}
