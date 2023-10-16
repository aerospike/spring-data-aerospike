package org.springframework.data.aerospike.utility;


import org.springframework.data.aerospike.query.FilterOperation;

public class FilterOperationRegexpBuilder {

    private static final Character BACKSLASH = '\\';
    private static final Character DOT = '.';
    private static final Character ASTERISK = '*';
    private static final Character DOLLAR = '$';
    private static final Character OPEN_BRACKET = '[';
    private static final Character CIRCUMFLEX = '^';

    private FilterOperationRegexpBuilder() {
    }

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
