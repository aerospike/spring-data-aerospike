package org.springframework.data.aerospike.utility;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ResponseUtils {

    public static <T> List<T> parseResponse(String response, Function<Map<String, String>, T> mapper) {
        return Arrays.stream(response.split(";"))
                .map(job -> Arrays.stream(job.split(":"))
                        .map(pair -> {
                            String[] kv = pair.split("=");
                            return new AbstractMap.SimpleEntry<>(kv[0], kv[1]);
                        })
                        .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue))
                ).map(mapper)
                .collect(Collectors.toList());
    }
}
