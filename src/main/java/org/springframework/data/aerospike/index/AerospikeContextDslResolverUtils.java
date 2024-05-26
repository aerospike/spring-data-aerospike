package org.springframework.data.aerospike.index;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.exp.Exp;

import java.util.List;

import static org.springframework.data.aerospike.index.AerospikeContextDslResolverUtils.CtxType.LIST_INDEX;
import static org.springframework.data.aerospike.index.AerospikeContextDslResolverUtils.CtxType.LIST_RANK;
import static org.springframework.data.aerospike.index.AerospikeContextDslResolverUtils.CtxType.LIST_VALUE;
import static org.springframework.data.aerospike.index.AerospikeContextDslResolverUtils.CtxType.MAP_INDEX;
import static org.springframework.data.aerospike.index.AerospikeContextDslResolverUtils.CtxType.MAP_KEY;
import static org.springframework.data.aerospike.index.AerospikeContextDslResolverUtils.CtxType.MAP_RANK;
import static org.springframework.data.aerospike.index.AerospikeContextDslResolverUtils.CtxType.MAP_VALUE;

public class AerospikeContextDslResolverUtils {

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
            throw new IllegalArgumentException(String.format("Context DSL: string '%s' has no content", singleCtx));
        }
        if (singleCtx.charAt(length - 1) != ctxType.closingChar) {
            throw new IllegalArgumentException(String.format("Context DSL: brackets mismatch, expecting '%s', " +
                "got '%s' instead", ctxType.closingChar, singleCtx.charAt(length - 1)));
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
            throw new IllegalArgumentException(String.format("Context DSL %s %s: expecting only integer values, " +
                "got '%s' instead", ctxType, parameterName, substring));
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

    /**
     * Check whether context element's id is the same as in {@link CTX#mapKey(Value)}
     */
    public static boolean isCtxMapKey(CTX ctx) {
        return ctx.id == MAP_KEY.getId();
    }

    /**
     * Check whether context element's id is the same as in {@link CTX#mapValue(Value)}
     */
    public static boolean isCtxMapValue(CTX ctx) {
        return ctx.id == MAP_VALUE.getId();
    }

    /**
     * Check whether context element's id is the same as in {@link CTX#mapKey(Value)}
     */
    public static Exp.Type getCtxType(CTX ctx) {
        List<Integer> listIds = List.of(LIST_INDEX.getId(), LIST_RANK.getId(), LIST_VALUE.getId());
        List<Integer> mapIds = List.of(MAP_INDEX.getId(), MAP_RANK.getId(), MAP_KEY.getId(), MAP_VALUE.getId());
        if (listIds.contains(ctx.id)) {
            return Exp.Type.LIST;
        } else if (mapIds.contains(ctx.id)) {
            return Exp.Type.MAP;
        } else {
            throw new IllegalStateException("Unexpected CTX element id");
        }
    }

    enum CtxType {
        LIST_INDEX(0x10),
        LIST_RANK(0x11),
        LIST_VALUE(0x13),
        MAP_INDEX(0x20),
        MAP_RANK(0x21),
        MAP_KEY(0x22),
        MAP_VALUE(0x23);

        private final int id;

        CtxType(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }
    }
}
