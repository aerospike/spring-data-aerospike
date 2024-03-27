/*
 * Copyright 2023 the original author or authors.
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
package org.springframework.data.aerospike.util;

import lombok.experimental.UtilityClass;
import org.springframework.data.aerospike.query.FilterOperation;

@UtilityClass
public class FilterOperationRegexpBuilder {

    private static final Character BACKSLASH = '\\';
    private static final Character DOT = '.';
    private static final Character ASTERISK = '*';
    private static final Character DOLLAR = '$';
    private static final Character OPEN_BRACKET = '[';
    private static final Character CIRCUMFLEX = '^';

    public static String escapeBRERegexp(String base) {
        StringBuilder builder = new StringBuilder();
        for (char stringChar : base.toCharArray()) {
            if (
                stringChar == BACKSLASH ||
                    stringChar == DOT ||
                    stringChar == ASTERISK ||
                    stringChar == DOLLAR ||
                    stringChar == OPEN_BRACKET ||
                    stringChar == CIRCUMFLEX) {
                builder.append(BACKSLASH);
            }
            builder.append(stringChar);
        }
        return builder.toString();
    }

    /*
     * This op is always in [START_WITH, ENDS_WITH, EQ, CONTAINING]
     */
    private static String getRegexp(String base, FilterOperation op) {
        String escapedBase = escapeBRERegexp(base);
        return switch (op) {
            case STARTS_WITH -> "^" + escapedBase;
            case ENDS_WITH -> escapedBase + "$";
            case EQ -> "^" + escapedBase + "$";
            default -> escapedBase;
        };
    }

    public static String getStartsWith(String base) {
        return getRegexp(base, FilterOperation.STARTS_WITH);
    }

    public static String getEndsWith(String base) {
        return getRegexp(base, FilterOperation.ENDS_WITH);
    }

    public static String getContaining(String base) {
        return getRegexp(base, FilterOperation.CONTAINING);
    }

    public static String getNotContaining(String base) {
        return getRegexp(base, FilterOperation.NOT_CONTAINING);
    }

    public static String getStringEquals(String base) {
        return getRegexp(base, FilterOperation.EQ);
    }
}
