package org.springframework.data.aerospike;

import com.aerospike.client.query.KeyRecord;
import org.springframework.data.aerospike.query.KeyRecordIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CollectionUtils {

	public static Collector<Integer, ?, Integer> countingInt() {
		return Collectors.reducing(0, e -> 1, Integer::sum);
	}

	public static <T> Stream<T> toStream(Iterator<T> it) {
		return StreamSupport.stream(
				Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED),
				false);
	}

	public static List<KeyRecord> toList(KeyRecordIterator it) {
		List<KeyRecord> result = new ArrayList<>();
		it.forEachRemaining(result::add);
		return result;
	}
}
