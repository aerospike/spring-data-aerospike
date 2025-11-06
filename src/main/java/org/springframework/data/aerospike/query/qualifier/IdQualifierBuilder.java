package org.springframework.data.aerospike.query.qualifier;

import static org.springframework.data.aerospike.query.qualifier.QualifierKey.MULTIPLE_IDS_EQ_FIELD;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.SINGLE_ID_EQ_FIELD;

/**
 * Builder for id query qualifier (transferring ids)
 **/
class IdQualifierBuilder extends BaseQualifierBuilder<IdQualifierBuilder> {

    IdQualifierBuilder() {
    }

    IdQualifierBuilder setId(String id) {
        this.map.put(SINGLE_ID_EQ_FIELD, id);
        return this;
    }

    IdQualifierBuilder setId(Short id) {
        this.map.put(SINGLE_ID_EQ_FIELD, id);
        return this;
    }

    IdQualifierBuilder setId(Integer id) {
        this.map.put(SINGLE_ID_EQ_FIELD, id);
        return this;
    }

    IdQualifierBuilder setId(Long id) {
        this.map.put(SINGLE_ID_EQ_FIELD, id);
        return this;
    }

    IdQualifierBuilder setId(Character id) {
        this.map.put(SINGLE_ID_EQ_FIELD, id);
        return this;
    }

    IdQualifierBuilder setId(Byte id) {
        this.map.put(SINGLE_ID_EQ_FIELD, id);
        return this;
    }

    IdQualifierBuilder setId(byte[] id) {
        this.map.put(SINGLE_ID_EQ_FIELD, id);
        return this;
    }

    IdQualifierBuilder setIds(String... ids) {
        this.map.put(MULTIPLE_IDS_EQ_FIELD, ids);
        return this;
    }

    IdQualifierBuilder setIds(Short... ids) {
        this.map.put(MULTIPLE_IDS_EQ_FIELD, ids);
        return this;
    }

    IdQualifierBuilder setIds(Integer... ids) {
        this.map.put(MULTIPLE_IDS_EQ_FIELD, ids);
        return this;
    }

    IdQualifierBuilder setIds(Long... ids) {
        this.map.put(MULTIPLE_IDS_EQ_FIELD, ids);
        return this;
    }

    IdQualifierBuilder setIds(Character... ids) {
        this.map.put(MULTIPLE_IDS_EQ_FIELD, ids);
        return this;
    }

    IdQualifierBuilder setIds(Byte... ids) {
        this.map.put(MULTIPLE_IDS_EQ_FIELD, ids);
        return this;
    }

    IdQualifierBuilder setIds(byte[]... ids) {
        this.map.put(MULTIPLE_IDS_EQ_FIELD, ids);
        return this;
    }
}
