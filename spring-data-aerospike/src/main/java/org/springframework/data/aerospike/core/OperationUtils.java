package org.springframework.data.aerospike.core;

import com.aerospike.client.Bin;
import com.aerospike.client.Operation;

import java.util.function.Function;

public class OperationUtils {

	public static Operation[] operations(Bin[] bins,
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
