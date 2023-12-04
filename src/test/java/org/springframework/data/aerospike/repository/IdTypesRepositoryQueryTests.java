package org.springframework.data.aerospike.repository;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.sample.DocumentIntegerIdRepository;
import org.springframework.data.aerospike.sample.DocumentLongIdRepository;
import org.springframework.data.aerospike.sample.DocumentShortIdRepository;
import org.springframework.data.aerospike.sample.DocumentStringIdRepository;
import org.springframework.data.aerospike.sample.SampleClasses.DocumentWithIntegerId;
import org.springframework.data.aerospike.sample.SampleClasses.DocumentWithLongId;
import org.springframework.data.aerospike.sample.SampleClasses.DocumentWithShortId;
import org.springframework.data.aerospike.sample.SampleClasses.DocumentWithStringId;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class IdTypesRepositoryQueryTests extends BaseBlockingIntegrationTests {

    @Autowired
    DocumentStringIdRepository documentStringIdRepository;
    @Autowired
    DocumentShortIdRepository documentShortIdRepository;
    @Autowired
    DocumentIntegerIdRepository documentIntegerIdRepository;
    @Autowired
    DocumentLongIdRepository documentLongIdRepository;

    DocumentWithStringId docStringId1 = DocumentWithStringId.builder().id("id1").content("contentString1").build();
    DocumentWithStringId docStringId2 = DocumentWithStringId.builder().id("id2").content("contentString2").build();
    DocumentWithStringId docStringId3 = DocumentWithStringId.builder().id("id3").content("contentString3").build();
    DocumentWithShortId docShortId1 = DocumentWithShortId.builder().id((short) 1).content("contentShort1").build();
    DocumentWithShortId docShortId2 = DocumentWithShortId.builder().id((short) 2).content("contentShort2").build();
    DocumentWithShortId docShortId3 = DocumentWithShortId.builder().id((short) 3).content("contentShort3").build();
    DocumentWithIntegerId docIntegerId1 = DocumentWithIntegerId.builder().id(1).content("contentInteger1").build();
    DocumentWithIntegerId docIntegerId2 = DocumentWithIntegerId.builder().id(2).content("contentInteger2").build();
    DocumentWithIntegerId docIntegerId3 = DocumentWithIntegerId.builder().id(3).content("contentInteger3").build();
    DocumentWithLongId docLongId1 = DocumentWithLongId.builder().id(1L).content("contentLong1").build();
    DocumentWithLongId docLongId2 = DocumentWithLongId.builder().id(2L).content("contentLong2").build();
    DocumentWithLongId docLongId3 = DocumentWithLongId.builder().id(3L).content("contentLong3").build();

    @BeforeAll
    void beforeAll() {
        documentStringIdRepository.saveAll(List.of(docStringId1, docStringId2, docStringId3));
        documentShortIdRepository.saveAll(List.of(docShortId1, docShortId2, docShortId3));
        documentIntegerIdRepository.saveAll(List.of(docIntegerId1, docIntegerId2, docIntegerId3));
        documentLongIdRepository.saveAll(List.of(docLongId1, docLongId2, docLongId3));
    }

    @AfterAll
    void afterAll() {
        documentStringIdRepository.deleteAll();
        documentShortIdRepository.deleteAll();
        documentIntegerIdRepository.deleteAll();
        documentLongIdRepository.deleteAll();
    }

    @Test
    void findUsingQueryStringIdEquals() {
        Qualifier idEquals = Qualifier.idEquals(docStringId1.getId());
        Iterable<DocumentWithStringId> result = documentStringIdRepository.findUsingQuery(new Query(idEquals));
        assertThat(result).containsOnly(docStringId1);
    }

    @Test
    void findUsingQueryShortIdEquals() {
        Qualifier idEquals = Qualifier.idEquals(docShortId1.getId());
        Iterable<DocumentWithShortId> result = documentShortIdRepository.findUsingQuery(new Query(idEquals));
        assertThat(result).containsOnly(docShortId1);
    }

    @Test
    void findUsingQueryIntegerIdEquals() {
        Qualifier idEquals = Qualifier.idEquals(docIntegerId1.getId());
        Iterable<DocumentWithIntegerId> result = documentIntegerIdRepository.findUsingQuery(new Query(idEquals));
        assertThat(result).containsOnly(docIntegerId1);
    }

    @Test
    void findUsingQueryLongIdEquals() {
        Qualifier idEquals = Qualifier.idEquals(docLongId1.getId());
        Iterable<DocumentWithLongId> result = documentLongIdRepository.findUsingQuery(new Query(idEquals));
        assertThat(result).containsOnly(docLongId1);
    }

    @Test
    void findUsingQueryStringIdIn() {
        Qualifier idIn = Qualifier.idIn(docStringId1.getId(), docStringId3.getId());
        Iterable<DocumentWithStringId> result = documentStringIdRepository.findUsingQuery(new Query(idIn));
        assertThat(result).containsOnly(docStringId1, docStringId3);
    }

    @Test
    void findUsingQueryShortIdIn() {
        Qualifier idIn = Qualifier.idIn(docShortId1.getId(), docShortId3.getId());
        Iterable<DocumentWithShortId> result = documentShortIdRepository.findUsingQuery(new Query(idIn));
        assertThat(result).containsOnly(docShortId1, docShortId3);
    }

    @Test
    void findUsingQueryIntegerIdIn() {
        Qualifier idIn = Qualifier.idIn(docIntegerId2.getId(), docIntegerId3.getId());
        Iterable<DocumentWithIntegerId> result = documentIntegerIdRepository.findUsingQuery(new Query(idIn));
        assertThat(result).containsOnly(docIntegerId2, docIntegerId3);
    }

    @Test
    void findUsingQueryLongIdIn() {
        Qualifier idIn = Qualifier.idIn(docLongId1.getId(), docLongId2.getId());
        Iterable<DocumentWithLongId> result = documentLongIdRepository.findUsingQuery(new Query(idIn));
        assertThat(result).containsOnly(docLongId1, docLongId2);
    }
}
