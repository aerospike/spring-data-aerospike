package org.springframework.data.aerospike.cache;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class CacheUtils {

    private CacheUtils() {
    }

    protected static byte[] serialize(Object key, Kryo kryoInstance) {
        ByteBufferOutput output = new ByteBufferOutput(1024); // Initial buffer size
        kryoInstance.writeClassAndObject(output, key);
        output.flush();
        return output.toBytes();
    }

    protected static String sha256(byte[] data) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(data);
        return bytesToHex(hash);
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (byte b : bytes) {
            result.append(String.format("%02x", b));
        }
        return result.toString();
    }
}
