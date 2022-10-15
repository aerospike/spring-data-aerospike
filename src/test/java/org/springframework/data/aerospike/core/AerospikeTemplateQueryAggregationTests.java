package org.springframework.data.aerospike.core;

import com.aerospike.client.Language;
import com.aerospike.client.Value;
import com.aerospike.client.lua.LuaConfig;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.task.RegisterTask;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.sample.Person;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AerospikeTemplateQueryAggregationTests extends BaseBlockingIntegrationTests {

    @BeforeAll
    public void setUp() {
        // Register UDFs
        RegisterTask taskSum = client.register(null,
                "src/test/resources/udf/sum_example.lua", "sum_example.lua", Language.LUA);
        taskSum.waitTillComplete();
        RegisterTask taskAvg = client.register(null,
                "src/test/resources/udf/average_example.lua", "average_example.lua", Language.LUA);
        taskAvg.waitTillComplete();

        // Set Lua config source directory.
        LuaConfig.SourceDirectory = "src/test/resources/udf";

        // Clean existing data
        additionalAerospikeTestOperations.deleteAll(Person.class);

        // Insert data
        Person firstPerson = Person.builder()
                .id(nextId())
                .firstName("first")
                .lastName("lastName1")
                .emailAddress("gmail.com")
                .age(40)
                .build();
        Person secondPerson = Person.builder()
                .id(nextId())
                .firstName("second")
                .lastName("lastName2")
                .emailAddress("gmail.com")
                .age(50)
                .build();
        Person thirdPerson = Person.builder()
                .id(nextId())
                .firstName("second")
                .lastName("lastName2")
                .emailAddress("gmail.com")
                .age(30)
                .build();
        template.save(firstPerson);
        template.save(secondPerson);
        template.save(thirdPerson);

        // Create index
        additionalAerospikeTestOperations.createIndexIfNotExists(Person.class,
                "person_age_index", "age", IndexType.NUMERIC);
    }

    @AfterAll
    public void cleanUp() {
        // Revert Lua config source directory
        LuaConfig.SourceDirectory = System.getProperty("lua.dir", "udf");

        // Clean existing data
        additionalAerospikeTestOperations.deleteAll(Person.class);
    }

    @Test
    public void queryAggregationSum() {
        String binName = "age";
        List<Value> args = new ArrayList<>();
        args.add(Value.get(binName));

        try (ResultSet rs = template.aggregate(null, Person.class,
                "sum_example", "sum_single_bin", args)) {
            while (rs.next()) {
                assertThat(rs.getObject()).isEqualTo(120L);
            }
        }
    }

    @Test
    public void queryAggregationSumWithFilter() {
        String binName = "age";
        List<Value> args = new ArrayList<>();
        args.add(Value.get(binName));
        Filter filter = Filter.range(binName, 35, 60);

        try (ResultSet rs = template.aggregate(filter, Person.class,
                "sum_example", "sum_single_bin", args)) {
            while (rs.next()) {
                assertThat(rs.getObject()).isEqualTo(90L);
            }
        }
    }

    @Test
    public void queryAggregationAvg() {
        String binName = "age";
        List<Value> args = new ArrayList<>();
        args.add(Value.get(binName));

        try (ResultSet rs = template.aggregate(null, Person.class,
                "average_example", "average", args)) {
            while (rs.next()) {
                Map<?, ?> map = (Map<?, ?>) rs.getObject();
                long sum = (Long) map.get("sum");
                long count = (Long) map.get("count");
                double avg = (double) sum / count;
                assertThat(avg).isEqualTo(40L);
            }
        }
    }

    @Test
    public void queryAggregationAvgWithFilter() {
        String binName = "age";
        List<Value> args = new ArrayList<>();
        args.add(Value.get(binName));
        Filter filter = Filter.range(binName, 35, 60);

        try (ResultSet rs = template.aggregate(filter, Person.class,
                "average_example", "average", args)) {
            while (rs.next()) {
                Map<?, ?> map = (Map<?, ?>) rs.getObject();
                long sum = (Long) map.get("sum");
                long count = (Long) map.get("count");
                double avg = (double) sum / count;
                assertThat(avg).isEqualTo(45L);
            }
        }
    }
}
