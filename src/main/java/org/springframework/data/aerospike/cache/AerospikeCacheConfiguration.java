package org.springframework.data.aerospike.cache;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class AerospikeCacheConfiguration {
    private final String namespace;
    private String set; // null: Default set is null meaning write directly to the namespace.
    private int expirationInSeconds; // 0: Default to namespace configuration variable "default-ttl" on the server.

    public AerospikeCacheConfiguration (String namespace) {
        this.namespace = namespace;
    }

    public AerospikeCacheConfiguration (String namespace, String set) {
        this.namespace = namespace;
        this.set = set;
    }

    public AerospikeCacheConfiguration (String namespace, int expirationInSeconds) {
        this.namespace = namespace;
        this.expirationInSeconds = expirationInSeconds;
    }
}
