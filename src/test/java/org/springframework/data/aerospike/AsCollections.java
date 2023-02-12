package org.springframework.data.aerospike;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AsCollections {

	public static <K, V> Map<K, V> of(Object... args) {
		if (args.length % 2 != 0) {
			throw new IllegalArgumentException("Only pairs are accepted. Current number of args: " + args.length);
		}
		Map<K, V> map = new HashMap<>();
		for (int i = 0; i < args.length; i = i + 2) {
			map.put((K) args[i], (V) args[i + 1]);
		}
		return map;
	}

	@SafeVarargs
	public static <T> Set<T> set(T... args) {
		return Stream.of(args).collect(Collectors.toSet());
	}

	@SafeVarargs
	public static <T> List<T> list(T... args) {
		return Stream.of(args).collect(Collectors.toList());
	}
}
