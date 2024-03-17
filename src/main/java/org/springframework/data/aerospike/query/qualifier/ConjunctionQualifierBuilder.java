package org.springframework.data.aerospike.query.qualifier;

import org.springframework.util.Assert;

import static org.springframework.data.aerospike.query.qualifier.QualifierKey.QUALIFIERS;

class ConjunctionQualifierBuilder extends BaseQualifierBuilder<ConjunctionQualifierBuilder> {

    ConjunctionQualifierBuilder() {
    }

    ConjunctionQualifierBuilder setQualifiers(Qualifier... qualifiers) {
        this.map.put(QUALIFIERS, qualifiers);
        return this;
    }

    Qualifier[] getQualifiers() {
        return (Qualifier[]) this.map.get(QUALIFIERS);
    }

    @Override
    protected void validate() {
        Assert.notNull(this.getQualifiers(), "Qualifiers must not be null");
        Assert.notEmpty(this.getQualifiers(), "Qualifiers must not be empty");
        Assert.isTrue(this.getQualifiers().length > 1, "There must be at least 2 qualifiers");
    }
}
