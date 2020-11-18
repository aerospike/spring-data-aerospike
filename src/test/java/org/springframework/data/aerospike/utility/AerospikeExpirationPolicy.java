package org.springframework.data.aerospike.utility;

import lombok.experimental.UtilityClass;

@UtilityClass
public class AerospikeExpirationPolicy {

    public static final int DO_NOT_UPDATE_EXPIRATION = -2;
    public static final int NEVER_EXPIRE = -1;
    public static final int NAMESPACE_DEFAULT_EXPIRATION = 0;
}
