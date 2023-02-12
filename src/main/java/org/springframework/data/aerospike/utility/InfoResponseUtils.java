/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.utility;

import lombok.experimental.UtilityClass;

import java.util.function.Function;
import java.util.regex.Pattern;

@UtilityClass
public class InfoResponseUtils {

    private static final Pattern COLON_DELIMITER = Pattern.compile(":");
    private static final Pattern SEMICOLON_DELIMITER = Pattern.compile(";");

    public static <T> T getPropertyFromInfoResponse(String response, String propertyName, Function<String, T> mapper) {
        return getPropertyFromResponse(response, propertyName, mapper, COLON_DELIMITER);
    }

    public static <T> T getPropertyFromConfigResponse(String response, String propertyName,
                                                      Function<String, T> mapper) {
        return getPropertyFromResponse(response, propertyName, mapper, SEMICOLON_DELIMITER);
    }

    public static <T> T getPropertyFromResponse(String response, String propertyName, Function<String, T> mapper,
                                                Pattern delimiterPattern) {
        String[] keyValuePair = delimiterPattern
            .splitAsStream(response)
            .filter(pair -> pair.startsWith(propertyName))
            .findFirst()
            .map(objectsStr -> objectsStr.split("="))
            .orElseThrow(() -> new IllegalStateException(
                "Failed to parse server response. Could not to find property: " + propertyName + " in response: " +
                    response));

        if (keyValuePair.length != 2) {
            throw new IllegalStateException("Failed to parse server response. Expected property: " + propertyName +
                " to have length 2 in response: " + response);
        }
        String valueStr = keyValuePair[1];
        try {
            return mapper.apply(valueStr);
        } catch (Exception e) {
            throw new IllegalStateException(
                "Failed to parse value: " + valueStr + " for property: " + propertyName + " in response: " + response);
        }
    }
}
