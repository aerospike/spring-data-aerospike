package org.springframework.data.aerospike.examples.reactive.converters;

import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.examples.reactive.converters.entity.ReactiveConverterOrderDocument;
import org.springframework.data.aerospike.examples.reactive.converters.entity.ReactiveConverterOrderId;
import org.springframework.stereotype.Component;

import java.util.List;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;

@Component
public class ReactiveCustomConvertersExample {

    private final ReactiveAerospikeTemplate template;

    public ReactiveCustomConvertersExample(ReactiveAerospikeTemplate template) {
        this.template = template;
    }

    public void run() {
        ReactiveConverterOrderId firstId = new ReactiveConverterOrderId("account-a", 1001);
        ReactiveConverterOrderId secondId = new ReactiveConverterOrderId("account-a", 1002);
        ReactiveConverterOrderId thirdId = new ReactiveConverterOrderId("account-b", 2001);

        template.save(new ReactiveConverterOrderDocument(firstId, "first converted-id order", 1)).block();

        ReactiveConverterOrderDocument found = template.findById(firstId, ReactiveConverterOrderDocument.class)
            .block();
        require(found != null, "Expected to read the saved reactive converted-id order");
        require("account-a".equals(found.getId().getAccountId()), "Reactive converted id account part did not round-trip");
        require(found.getId().getOrderNumber() == 1001, "Reactive converted id order number did not round-trip");

        template.insertAll(List.of(
                new ReactiveConverterOrderDocument(secondId, "second converted-id order", 2),
                new ReactiveConverterOrderDocument(thirdId, "third converted-id order", 3)
            ))
            .collectList()
            .block();

        List<ReactiveConverterOrderDocument> orders = template
            .findByIds(List.of(firstId, secondId, thirdId), ReactiveConverterOrderDocument.class)
            .collectList()
            .block();
        require(orders != null && orders.size() == 3, "Expected all reactive converted-id orders from findByIds");
        require(Boolean.TRUE.equals(template.exists(secondId, ReactiveConverterOrderDocument.class).block()),
            "Expected reactive converted id to be usable in exists");

        Boolean deleted = template.deleteById(firstId, ReactiveConverterOrderDocument.class).block();
        require(Boolean.TRUE.equals(deleted), "Expected reactive converted id to be usable in deleteById");
        require(!Boolean.TRUE.equals(template.exists(firstId, ReactiveConverterOrderDocument.class).block()),
            "Deleted reactive converted-id order should be gone");

        System.out.println("Ran reactive custom converter save, find, findByIds, exists, and delete operations");
    }
}
