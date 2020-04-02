package org.springframework.data.aerospike.convert;

import com.aerospike.client.Bin;
import com.aerospike.client.Record;
import org.springframework.context.ApplicationContext;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.env.Environment;
import org.springframework.data.aerospike.SampleClasses;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.convert.CustomConversions;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class BaseMappingAerospikeConverterTest {

	protected static final String NAMESPACE = "namespace";

	protected MappingAerospikeConverter converter = getMappingAerospikeConverter(
			new SampleClasses.ComplexIdToStringConverter(),
			new SampleClasses.StringToComplexIdConverter());

	protected MappingAerospikeConverter getMappingAerospikeConverter(Converter<?, ?>... customConverters) {
		return getMappingAerospikeConverter(new AerospikeTypeAliasAccessor(), customConverters);
	}

	protected MappingAerospikeConverter getMappingAerospikeConverter(AerospikeTypeAliasAccessor typeAliasAccessor, Converter<?, ?>... customConverters) {
		AerospikeMappingContext mappingContext = new AerospikeMappingContext();
		mappingContext.setApplicationContext(getApplicationContext());
		mappingContext.setDefaultNameSpace(NAMESPACE);
		CustomConversions customConversions = new AerospikeCustomConversions(asList(customConverters));

		MappingAerospikeConverter converter = new MappingAerospikeConverter(mappingContext, customConversions, typeAliasAccessor);
		converter.afterPropertiesSet();
		return converter;
	}

	private ApplicationContext getApplicationContext() {
		Environment environment = mock(Environment.class);
		when(environment.resolveRequiredPlaceholders(anyString()))
				.thenAnswer(invocationOnMock -> invocationOnMock.getArgument(0));

		ApplicationContext applicationContext = mock(ApplicationContext.class);
		when(applicationContext.getEnvironment()).thenReturn(environment);

		return applicationContext;
	}

	protected static Record record(Collection<Bin> bins) {
		Map<String, Object> collect = bins.stream()
				.collect(Collectors.toMap(bin -> bin.name, bin -> bin.value.getObject()));
		return record(collect);
	}

	protected static Record record(Map<String, Object> bins) {
		return new Record(bins, 0, 0);
	}
}
