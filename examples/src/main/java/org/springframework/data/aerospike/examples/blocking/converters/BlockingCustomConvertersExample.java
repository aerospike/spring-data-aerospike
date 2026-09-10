package org.springframework.data.aerospike.examples.blocking.converters;

import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.examples.blocking.converters.entity.ConverterOrderDocument;
import org.springframework.data.aerospike.examples.blocking.converters.entity.ConverterOrderId;
import org.springframework.stereotype.Component;

import java.util.List;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;

@Component
public class BlockingCustomConvertersExample {

    private final AerospikeTemplate template;

    public BlockingCustomConvertersExample(AerospikeTemplate template) {
        this.template = template;
    }

    public void run() {
        ConverterOrderId firstId = new ConverterOrderId("account-a", 1001);
        ConverterOrderId secondId = new ConverterOrderId("account-a", 1002);
        ConverterOrderId thirdId = new ConverterOrderId("account-b", 2001);

        template.save(new ConverterOrderDocument(firstId, "first converted-id order", 1));

        ConverterOrderDocument found = template.findById(firstId, ConverterOrderDocument.class);
        require(found != null, "Expected to read the saved converted-id order");
        require("account-a".equals(found.getId().getAccountId()), "Converted id account part did not round-trip");
        require(found.getId().getOrderNumber() == 1001, "Converted id order number did not round-trip");

        template.insertAll(List.of(
            new ConverterOrderDocument(secondId, "second converted-id order", 2),
            new ConverterOrderDocument(thirdId, "third converted-id order", 3)
        ));

        List<ConverterOrderDocument> orders = template.findByIds(List.of(firstId, secondId, thirdId),
            ConverterOrderDocument.class);
        require(orders.size() == 3, "Expected all converted-id orders from findByIds");
        require(template.exists(secondId, ConverterOrderDocument.class), "Expected converted id to be usable in exists");

        boolean deleted = template.deleteById(firstId, ConverterOrderDocument.class);
        require(deleted, "Expected converted id to be usable in deleteById");
        require(!template.exists(firstId, ConverterOrderDocument.class), "Deleted converted-id order should be gone");

        System.out.println("Ran blocking custom converter save, find, findByIds, exists, and delete operations");
    }
}
