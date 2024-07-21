/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	  https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.query.cache;

import com.aerospike.client.IAerospikeClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.query.model.IndexKey;
import org.springframework.data.aerospike.query.model.IndexesInfo;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.data.aerospike.util.InfoCommandUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toMap;

/**
 * Internal index related operations used by ReactorIndexRefresher and IndexRefresher.
 *
 * @author Sergii Karpenko
 */
@Slf4j
public class InternalIndexOperations {

    // Base64 will return index context as a base64 response
    private static final String SINDEX_WITH_BASE64 = "sindex-list:;b64=true";
    private final IndexInfoParser indexInfoParser;

    public InternalIndexOperations(IndexInfoParser indexInfoParser) {
        this.indexInfoParser = indexInfoParser;
    }

    private static IndexKey getIndexKey(Index index) {
        return new IndexKey(index.getNamespace(), index.getSet(), index.getBin(), index.getIndexType(),
            index.getIndexCollectionType(), index.getCtx());
    }

    public IndexesInfo parseIndexesInfo(String infoResponse) {
        if (infoResponse.isEmpty()) {
            return IndexesInfo.empty();
        }
        return IndexesInfo.of(Arrays.stream(infoResponse.split(";"))
            .map(indexInfoParser::parse)
            .collect(collectingAndThen(
                toMap(InternalIndexOperations::getIndexKey, index -> index),
                Collections::unmodifiableMap)));
    }

    public String buildGetIndexesCommand() {
        return SINDEX_WITH_BASE64;
    }

    public void enrichIndexesWithCardinality(IAerospikeClient client, Map<IndexKey, Index> indexes,
                                             ServerVersionSupport serverVersionSupport) {
        log.debug("Enriching secondary indexes with cardinality");
        indexes.values().forEach(
            index -> index.setBinValuesRatio(getIndexBinValuesRatio(client, serverVersionSupport, index.getNamespace(),
                index.getName()))
        );
    }

    public int getIndexBinValuesRatio(IAerospikeClient client, ServerVersionSupport serverVersionSupport,
                                      String namespace, String indexName) {
        if (serverVersionSupport.isSIndexCardinalitySupported()) {
            try {
                String indexStatData = InfoCommandUtils.request(client, client.getCluster().getRandomNode(),
                    String.format("sindex-stat:ns=%s;indexname=%s", namespace, indexName));

                return Integer.parseInt(
                    Arrays.stream(indexStatData.split(";"))
                        .map(String::trim)
                        .toList().stream()
                        .map(stat -> Arrays.stream(stat.split("="))
                            .map(String::trim)
                            .collect(Collectors.toList()))
                        .collect(Collectors.toMap(t -> t.get(0), t -> t.get(1)))
                        .get("entries_per_bval"));
            } catch (Exception e) {
                log.warn("Failed to fetch secondary index {} cardinality", indexName, e);
            }
        }
        return 0;
    }
}
