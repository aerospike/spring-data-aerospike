package org.springframework.data.aerospike.cache;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Default set is null meaning write directly to the namespace.
 * Default expiration is 0 meaning use the server's default namespace configuration variable "default-ttl".
 */
@Getter
@AllArgsConstructor
public class AerospikeCacheConfiguration {
    private final String namespace;
    private final String set;
    private final int expirationInSeconds;

    public AerospikeCacheConfiguration (String namespace) {
        this(namespace, null, 0);
    }

    public AerospikeCacheConfiguration (String namespace, String set) {
        this(namespace, set, 0);
    }

    public AerospikeCacheConfiguration (String namespace, int expirationInSeconds) {
        this(namespace, null, expirationInSeconds);
    }
}
