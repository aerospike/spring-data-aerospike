package org.springframework.data.aerospike.query.qualifier;

/**
 * Key for storing data within Qualifier map
 */
public enum QualifierKey {

    PATH,
    BIN_NAME,
    BIN_TYPE,
    METADATA_FIELD,
    SINGLE_ID_FIELD,
    MULTIPLE_IDS_FIELD,
    DOT_PATH,
    CTX_LIST,
    CTX_ARRAY,
    IGNORE_CASE,
    KEY,
    NESTED_KEY,
    VALUE,
    SECOND_VALUE,
    NESTED_TYPE,
    DATA_SETTINGS,
    QUALIFIERS,
    FILTER_OPERATION,
    DIGEST_KEY,
    HAS_SINDEX_FILTER,
    SERVER_VERSION_SUPPORT
}
