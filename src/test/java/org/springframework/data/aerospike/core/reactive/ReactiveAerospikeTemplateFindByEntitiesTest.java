package org.springframework.data.aerospike.core.reactive;

import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.core.AbstractFindByEntitiesTest;
import org.springframework.data.aerospike.core.model.GroupedEntities;
import org.springframework.data.aerospike.core.model.GroupedKeys;
import org.springframework.test.context.TestPropertySource;
import reactor.core.scheduler.Schedulers;

import static org.springframework.data.aerospike.query.cache.IndexRefresher.INDEX_CACHE_REFRESH_SECONDS;

@TestPropertySource(properties = {INDEX_CACHE_REFRESH_SECONDS + " = 0", "createIndexesOnStartup = false"})
// this test class does not require secondary indexes created on startup
public class ReactiveAerospikeTemplateFindByEntitiesTest
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
