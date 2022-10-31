# Spring Data Aerospike [![maven][maven-image]][maven-url] [![ci][ci-image]][ci-url] [![javadoc][javadoc-image]][javadoc-url]

[maven-image]: https://img.shields.io/maven-central/v/com.aerospike/spring-data-aerospike.svg?maxAge=259200
[maven-url]: https://search.maven.org/#search%7Cga%7C1%7Ca%3A%22spring-data-aerospike%22
[ci-image]: https://github.com/aerospike-community/spring-data-aerospike/workflows/Build%20project/badge.svg
[ci-url]: https://github.com/aerospike-community/spring-data-aerospike/actions?query=branch%3Amain
[javadoc-image]: https://javadoc.io/badge2/com.aerospike/spring-data-aerospike/javadoc.svg 
[javadoc-url]: https://javadoc.io/doc/com.aerospike/spring-data-aerospike

The primary goal of the [Spring Data](https://projects.spring.io/spring-data) project is to make it easier to build Spring-powered applications that use new data access technologies such as non-relational databases, map-reduce frameworks, and cloud based data services.

The Spring Data Aerospike project aims to provide a familiar and consistent Spring-based programming model for new data stores while retaining store-specific features and capabilities. The Spring Data Aerospike project provides integration with the Aerospike document database. Key functional areas of Spring Data Aerospike are a POJO centric model for interacting with an Aerospike DBCollection and easily writing a repository style data access layer.

## Documentation

The documentation for this project can be found on [javadoc.io](https://www.javadoc.io/doc/com.aerospike/spring-data-aerospike).

## Demo Projects

1. Demo project with detailed guides is located [here](https://github.com/aerospike-community/spring-data-aerospike-demo).

2. Demo project example with a step-by-step tutorial can be found [here](https://github.com/aerospike-examples/simple-springboot-aerospike-demo).

## Getting Started blog posts

1. [Simple Web Application Using Java, Spring Boot, Aerospike and Docker](https://medium.com/aerospike-developer-blog/simple-web-application-using-java-spring-boot-aerospike-database-and-docker-ad13795e0089?source=friends_link&sk=43d747f5f55e527248125eeb18748d92)
2. [How to setup spring-data-aerospike in Spring Boot application](https://medium.com/aerospike-developer-blog/how-to-setup-spring-data-aerospike-in-spring-boot-application-afa8bcb59224?source=friends_link&sk=e16a3b69c814bfb22f200634c743e476)
3. [Basic error handling in spring-data-aerospike](https://medium.com/aerospike-developer-blog/basic-error-handling-in-spring-data-aerospike-5edd580d77d9?source=friends_link&sk=cff71ea1539b36e5a89b2c3411b58a06)
4. [How to create secondary index in Spring Data Aerospike](https://medium.com/aerospike-developer-blog/how-to-create-secondary-index-in-spring-data-aerospike-e19d7e343d7c?source=friends_link&sk=413619a568f9aac51ed2f2611ee70aba)
5. [Caching with Spring Boot and Aerospike](https://medium.com/aerospike-developer-blog/caching-with-spring-boot-and-aerospike-17b91267d6c?source=friends_link&sk=e166b4592c9c00e3d996663f4c47e2b5)
6. [Spring Data Aerospike: Reactive Repositories](https://medium.com/aerospike-developer-blog/spring-data-aerospike-reactive-repositories-fb6478acea41?source=friends_link&sk=66541b82192ded459a537261e9a38bd5)
7. [Spring Data Aerospike - Projections](https://medium.com/aerospike-developer-blog/spring-data-aerospike-projections-951382bc07b5?source=friends_link&sk=d0a3be4fd171bbc9e072d09ccbcf056f)

## Spring Data Aerospike compatibility

|Spring Data Aerospike | Spring Boot | Aerospike Client | Aerospike Reactor Client | Aerospike Server
| :----------- | :---- | :----------- | :----------- | :-----------
|3.5.x | 2.7.x | 6.1.x | 6.1.x | 5.2.x.x +
|3.4.x | 2.6.x | 5.1.x | 5.1.x | 5.2.x.x +
|3.3.x | 2.5.x | 5.1.x | 5.1.x | 5.2.x.x +
|3.2.x | 2.5.x | 5.1.x | 5.0.x | 5.2.x.x +
|3.0.x, 3.1.x | 2.5.x | 5.1.x | 5.0.x
|2.5.x | 2.5.x | 4.4.x | 4.4.x 
|2.4.2.RELEASE | 2.3.x | 4.4.x | 4.4.x
|2.3.5.RELEASE | 2.2.x | 4.4.x | 4.4.x
|2.1.1.RELEASE | 2.1.x, 2.0.x | 4.4.x | 3.2.x
|1.2.1.RELEASE | 1.5.x | 4.1.x | 

## Quick Start

### Maven configuration

Add the Maven dependency:

```xml
<dependency>
  <groupId>com.aerospike</groupId>
  <artifactId>spring-data-aerospike</artifactId>
  <version>3.5.0</version>
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
public interface PersonRepository extends AerospikeRepository<Person, Long> {

    List<Person> findByLastname(String lastname);

    List<Person> findByFirstnameLike(String firstname);
}
```

The queries issued on execution will be derived from the method name. Extending `AerospikeRepository` causes CRUD methods being pulled into the interface so that you can easily save and find single entities and collections of them.

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
        repository.save(person);

        List<Person> lastNameResults = repository.findByLastname("Gierke");
        List<Person> firstNameResults = repository.findByFirstnameLike("Oli*");
    }
}
```

## Getting Help

For a comprehensive treatment of all the Spring Data Aerospike features, please refer to:

* the [User Guide](https://github.com/aerospike-community/spring-data-aerospike/blob/main/src/main/asciidoc/index.adoc)
* for more detailed questions, use [Spring Data Aerospike on Stackoverflow](https://stackoverflow.com/questions/tagged/spring-data-aerospike).

If you are new to Spring as well as to Spring Data, look for information about [Spring projects](https://projects.spring.io/).

## Contributing to Spring Data

Here are some ways for you to get involved in the community:

* Get involved with the Spring community on Stackoverflow and help out on the [spring-data-aerospike](https://stackoverflow.com/questions/tagged/spring-data-aerospike) tag by responding to questions and joining the debate.
* Create [Github issue](https://github.com/aerospike-community/spring-data-aerospike/issues) for bugs and new features and comment and vote on the ones that you are interested in. 
* Github is for social coding: if you want to write code, we encourage contributions through pull requests from [forks of this repository](https://help.github.com/forking/). If you want to contribute code this way, please reference a Github ticket as well covering the specific issue you are addressing.
* Watch for upcoming articles by [subscribing](https://www.aerospike.com/forms/subscribe-the-aerospike-standup/) to Aerospike Standup.
