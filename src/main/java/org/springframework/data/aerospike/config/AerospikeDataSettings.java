package org.springframework.data.aerospike.config;

import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class AerospikeDataSettings {

	@Builder.Default
	boolean scansEnabled = false;
}
