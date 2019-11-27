package org.springframework.data.aerospike.core;

import com.aerospike.client.Bin;
import com.aerospike.client.Operation;
import com.aerospike.client.Value;

import java.util.Map;
import java.util.function.Function;

public class OperationUtils {

	static <T> Operation[] operations(Map<String, T> values,
									  Operation.Type operationType,
									  Operation... additionalOperations) {
		Operation[] operations = new Operation[values.size() + additionalOperations.length];
		int x = 0;
		for (Map.Entry<String, T> entry : values.entrySet()) {
			operations[x] = new Operation(operationType, entry.getKey(), Value.get(entry.getValue()));
			x++;
		}
		for (Operation additionalOp : additionalOperations) {
			operations[x] = additionalOp;
			x++;
		}
		return operations;
	}

	static Operation[] operations(Bin[] bins,
								  Function<Bin, Operation> binToOperation,
								  Operation... additionalOperations) {
		Operation[] operations = new Operation[bins.length + additionalOperations.length];
		int i = 0;
		for (Bin bin : bins) {
			operations[i] = binToOperation.apply(bin);
			i++;
		}
		for (Operation additionalOp : additionalOperations) {
			operations[i] = additionalOp;
			i++;
		}
		return operations;
	}
}
