package org.springframework.data.aerospike.core.reactive;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import org.junit.Test;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.aerospike.core.Person;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for different methods in {@link ReactiveAerospikeTemplate}.
 *
 * @author Igor Ermolenko
 */
public class ReactiveAerospikeTemplateMiscTests extends BaseReactiveAerospikeTemplateTests {

    @Test(expected = DuplicateKeyException.class)
    public void execute_shouldTranslateException() {
        Key key = new Key(template.getNamespace(), "shouldTranslateException", "reactiveShouldTranslateException");
        Bin bin = new Bin("bin_name", "bin_value");

        AerospikeClient client = template.getAerospikeClient();
        client.add(null, key, bin);
        reactiveTemplate.execute(() -> {
            WritePolicy writePolicy = new WritePolicy(client.getWritePolicyDefault());
            writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
            client.add(writePolicy, key, bin);
            return true;
        }).block();
    }

    public void exists_shouldReturnTrueIfValueIsPresent() {
        Person one = Person.builder().id(id).firstName("tya").emailAddress("gmail.com").build();
        reactiveTemplate.insert(one).block();

        assertThat(reactiveTemplate.exists(id, Person.class).block()).isTrue();
    }

    @Test
    public void exists_shouldReturnFalseIfValueIsAbsent() {
        assertThat(reactiveTemplate.exists(id, Person.class).block()).isFalse();
    }

}
