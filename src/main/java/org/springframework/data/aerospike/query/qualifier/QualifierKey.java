package org.springframework.data.aerospike.query.qualifier;

/**
 * Key for storing data within Qualifier map
 */
public enum QualifierKey {

    FIELD,
    METADATA_FIELD,
    SINGLE_ID_FIELD,
    MULTIPLE_IDS_FIELD,
    IGNORE_CASE,
    KEY,
    SECOND_KEY,
    VALUE,
    SECOND_VALUE,
    FIELD_TYPE,
    DOT_PATH,
    DATA_SETTINGS,
    QUALIFIERS,
    OPERATION,
    DIGEST_KEY,
    AS_FILTER
}
