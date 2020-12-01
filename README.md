# Spring Data Aerospike [![maven][maven-image]][maven-url] [![ci][ci-image]][ci-url]

[maven-image]: https://img.shields.io/maven-central/v/com.aerospike/spring-data-aerospike.svg?maxAge=259200
[maven-url]: https://search.maven.org/#search%7Cga%7C1%7Ca%3A%22spring-data-aerospike%22
[ci-image]: https://github.com/aerospike-community/spring-data-aerospike/workflows/Build%20project/badge.svg
[ci-url]: https://github.com/aerospike-community/spring-data-aerospike/actions?query=branch%3Amaster

The primary goal of the [Spring Data](https://projects.spring.io/spring-data) project is to make it easier to build Spring-powered applications that use new data access technologies such as non-relational databases, map-reduce frameworks, and cloud based data services.

The Spring Data Aerospike project aims to provide a familiar and consistent Spring-based programming model for new datastores while retaining store-specific features and capabilities. The Spring Data Aerospike project provides integration with the Aerospike document database. Key functional areas of Spring Data Aerospike are a POJO centric model for interacting with a Aerospike DBCollection and easily writing a repository style data access layer.

## :point_right: :point_right: :point_right: Demo project with guides

Demo project with detailed guides is located [here](https://github.com/aerospike-community/spring-data-aerospike-demo).

## Spring Boot compatibility

|`spring-data-aerospike` Version | Spring Boot Version
| :----------- | :----: |
|2.4.1.RELEASE | 2.3.x
|2.3.5.RELEASE | 2.2.x
|2.1.1.RELEASE | 2.1.x, 2.0.x
|1.2.1.RELEASE | 1.5.x

## Quick Start

### Maven configuration

Add the Maven dependency:

```xml
<dependency>
  <groupId>com.aerospike</groupId>
  <artifactId>spring-data-aerospike</artifactId>
  <version>2.4.1.RELEASE</version>
</dependency>
```

The Aerospike Spring Data connector depends on the Aerospike Client project:

```xml
<dependency>
  <groupId>com.aerospike</groupId>
  <artifactId>aerospike-client</artifactId>
</dependency>
```
Dependency will be provided for you by `spring-data-aerospike`, so no need to declare it additionally.
 
### AerospikeTemplate

`AerospikeTemplate` is the central support class for Aerospike database operations. It provides:

* Basic POJO mapping support to and from Bins
* Convenience methods to interact with the store (insert object, update objects) and Aerospike specific ones.
* Connection affinity callback
* Exception translation into Spring's [technology agnostic DAO exception hierarchy](https://docs.spring.io/spring/docs/current/spring-framework-reference/html/dao.html#dao-exceptions).

### Spring Data repositories

To simplify the creation of data repositories Spring Data Aerospike provides a generic repository programming model. It will automatically create a repository proxy for you that adds implementations of finder methods you specify on an interface.  

For example, given a `Person` class with first and last name properties, a `PersonRepository` interface that can query for `Person` by last name and when the first name matches a like expression is shown below:

```java
public interface PersonRepository extends CrudRepository<Person, Long> {

  List<Person> findByLastname(String lastname);

  List<Person> findByFirstnameLike(String firstname);
}
```

The queries issued on execution will be derived from the method name. Extending `CrudRepository` causes CRUD methods being pulled into the interface so that you can easily save and find single entities and collections of them.

You can have Spring automatically create a proxy for the interface by using the following JavaConfig:

```java
@Configuration
@EnableAerospikeRepositories(basePackageClasses = PersonRepository.class)
class ApplicationConfig extends AbstractAerospikeDataConfiguration {
	
	@Override
    protected Collection<Host> getHosts() {
    	return Collections.singleton(new Host("localhost", 3000));
    }
    
    @Override
    protected String nameSpace() {
    	return "TEST";
    }
	
}
```

This sets up a connection to a local Aerospike instance and enables the detection of Spring Data repositories (through `@EnableAerospikeRepositories`).

This will find the repository interface and register a proxy object in the container. You can use it as shown below:

```java
@Service
public class MyService {

  private final PersonRepository repository;

  @Autowired
  public MyService(PersonRepository repository) {
    this.repository = repository;
  }

  public void doWork() {

     repository.deleteAll();

     Person person = new Person();
     person.setFirstname("Oliver");
     person.setLastname("Gierke");
     person = repository.save(person);

     List<Person> lastNameResults = repository.findByLastname("Gierke");
     List<Person> firstNameResults = repository.findByFirstnameLike("Oli*");
 }
}
```

## Getting Help

For a comprehensive treatment of all the Spring Data Aerospike features, please refer to:

* the [User Guide](https://github.com/aerospike-community/spring-data-aerospike/blob/master/src/main/asciidoc/index.adoc)
* for more detailed questions, use [Spring Data Aerospike on Stackoverflow](https://stackoverflow.com/questions/tagged/spring-data-aerospike).

If you are new to Spring as well as to Spring Data, look for information about [Spring projects](https://projects.spring.io/).

## Contributing to Spring Data

Here are some ways for you to get involved in the community:

* Get involved with the Spring community on Stackoverflow and help out on the [spring-data-aerospike](https://stackoverflow.com/questions/tagged/spring-data-aerospike) tag by responding to questions and joining the debate.
* Create [Github issue](https://github.com/aerospike-community/spring-data-aerospike/issues) for bugs and new features and comment and vote on the ones that you are interested in. 
* Github is for social coding: if you want to write code, we encourage contributions through pull requests from [forks of this repository](https://help.github.com/forking/). If you want to contribute code this way, please reference a Github ticket as well covering the specific issue you are addressing.
* Watch for upcoming articles by [subscribing](https://dev.to/aerospike) to Aerospike Blog.
