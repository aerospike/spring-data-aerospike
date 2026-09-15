package org.springframework.data.aerospike.examples.blocking.indexed.context.entity;

// tag::indexed-context-friend[]
public class IndexedFriend {

    private String name;
    private IndexedAddress address;

    public IndexedFriend() {
    }

    public IndexedFriend(String name, IndexedAddress address) {
        this.name = name;
        this.address = address;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public IndexedAddress getAddress() {
        return address;
    }

    public void setAddress(IndexedAddress address) {
        this.address = address;
    }
}
// end::indexed-context-friend[]
