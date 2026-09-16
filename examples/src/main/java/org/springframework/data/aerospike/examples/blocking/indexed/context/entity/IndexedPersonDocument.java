package org.springframework.data.aerospike.examples.blocking.indexed.context.entity;

import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import org.springframework.data.aerospike.annotation.Indexed;
import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.annotation.Id;

// tag::indexed-context-person[]
@Document(collection = "sda_examples_indexed_context_people")
public class IndexedPersonDocument {

    public static final String FRIEND_ADDRESS_KEYS_INDEX = "sda_examples_friend_address_keys_idx";

    @Id
    private String id;

    // tag::indexed-context-field[]
    @Indexed(type = IndexType.STRING, name = FRIEND_ADDRESS_KEYS_INDEX,
        collectionType = IndexCollectionType.MAPKEYS, ctx = "address")
    private IndexedFriend friend;
    // end::indexed-context-field[]

    public IndexedPersonDocument() {
    }

    public IndexedPersonDocument(String id, IndexedFriend friend) {
        this.id = id;
        this.friend = friend;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public IndexedFriend getFriend() {
        return friend;
    }

    public void setFriend(IndexedFriend friend) {
        this.friend = friend;
    }
}
// end::indexed-context-person[]
