package org.springframework.data.aerospike.query.qualifier;

/**
 * Key for storing data within Qualifier map
 */
public enum QualifierKey {

    PATH,
    BIN_NAME,
    BIN_TYPE,
    METADATA_FIELD,
    SINGLE_ID_EQ_FIELD,
    MULTIPLE_IDS_EQ_FIELD,
    IS_ID_EXPR,
    DOT_PATH,
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
    SERVER_VERSION_SUPPORT,
    MAP_KEY_PLACEHOLDER
}
