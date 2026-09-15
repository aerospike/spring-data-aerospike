package org.springframework.data.aerospike.examples.blocking.indexed.context.entity;

// tag::indexed-context-address[]
public class IndexedAddress {

    private String street;
    private Integer apartment;
    private String zipCode;
    private String city;

    public IndexedAddress() {
    }

    public IndexedAddress(String street, Integer apartment, String zipCode, String city) {
        this.street = street;
        this.apartment = apartment;
        this.zipCode = zipCode;
        this.city = city;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public Integer getApartment() {
        return apartment;
    }

    public void setApartment(Integer apartment) {
        this.apartment = apartment;
    }

    public String getZipCode() {
        return zipCode;
    }

    public void setZipCode(String zipCode) {
        this.zipCode = zipCode;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
}
// end::indexed-context-address[]
