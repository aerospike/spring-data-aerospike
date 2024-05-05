package org.springframework.data.aerospike.index;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;

public class AerospikeIndexResolverUtils {

    public static CTX toCtx(String singleCtx) {
        switch (singleCtx.charAt(0)) {
            case '{' -> {
                return processSingleCtx(singleCtx, AerospikeIndexResolver.CtxType.MAP);
            }
            case '[' -> {
                return processSingleCtx(singleCtx, AerospikeIndexResolver.CtxType.LIST);
            }
            default -> {
                Object res = isInDoubleOrSingleQuotes(singleCtx) ? singleCtx.substring(1, singleCtx.length() - 1) :
                    parseIntOrReturnStr(singleCtx);
                return CTX.mapKey(Value.get(res));
            }
        }
    }

    private static CTX processSingleCtx(String singleCtx, AerospikeIndexResolver.CtxType ctxType) {
        int length = singleCtx.length();
        if (length < 3) {
            throw new IllegalArgumentException("@Indexed annotation: context string '" + singleCtx +
                "' has no content");
        }
        if (singleCtx.charAt(length - 1) != ctxType.closingChar) {
            throw new IllegalArgumentException("@Indexed annotation: brackets mismatch, " +
                "expecting '" + ctxType.closingChar + "', got '" + singleCtx.charAt(length - 1) + "' instead");
        }

        CTX result;
        String substring = singleCtx.substring(2, length - 1);
        if (singleCtx.charAt(1) == '=' && length > 3) {
            result = processCtxValue(substring, ctxType);
        } else if (singleCtx.charAt(1) == '#' && length > 3) {
            result = processCtxRank(substring, ctxType);
        } else {
            result = processCtxIndex(singleCtx, length, ctxType);
        }

        return result;
    }

    private static CTX processCtxValue(String substring, AerospikeIndexResolver.CtxType ctxType) {
        Object result = isInDoubleOrSingleQuotes(substring) ? substring.substring(1, substring.length() - 1) :
            parseIntOrReturnStr(substring);
        return switch (ctxType) {
            case MAP -> CTX.mapValue(Value.get(result));
            case LIST -> CTX.listValue(Value.get(result));
        };
    }

    private static CTX processCtxRank(String substring, AerospikeIndexResolver.CtxType ctxType) {
        int rank = parseIntOrFail(substring, ctxType, "rank");
        return switch (ctxType) {
            case MAP -> CTX.mapRank(rank);
            case LIST -> CTX.listRank(rank);
        };
    }

    private static CTX processCtxIndex(String singleCtx, int length, AerospikeIndexResolver.CtxType ctxType) {
        String substring = singleCtx.substring(1, length - 1);
        int idx = parseIntOrFail(substring, ctxType, "index");
        return switch (ctxType) {
            case MAP -> CTX.mapIndex(idx);
            case LIST -> CTX.listIndex(idx);
        };
    }

    private static int parseIntOrFail(String substring, AerospikeIndexResolver.CtxType ctxType, String parameterName) {
        try {
            return Integer.parseInt(substring);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("@Indexed annotation " + ctxType + " " + parameterName + ": " +
                "expecting only integer values, got '" + substring + "' instead");
        }
    }

    private static Object parseIntOrReturnStr(String str) {
        Object res;
        try {
            res = Integer.parseInt(str);
        } catch (NumberFormatException e) {
            res = str;
        }

        return res;
    }

    private static boolean isInDoubleOrSingleQuotes(String str) {
        return str.length() > 2 && (str.charAt(0) == '"' || str.charAt(0) == '\'')
            && (str.charAt(str.length() - 1) == '"' || str.charAt(str.length() - 1) == '\'');
    }
}
