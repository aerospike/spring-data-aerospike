/**
 * 
 */
package org.springframework.data.aerospike.core;

import org.springframework.data.aerospike.convert.AerospikeWriteData;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.convert.EntityWriter;
import org.springframework.data.util.TypeInformation;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public interface AerospikeWriter<T> extends EntityWriter<T, AerospikeWriteData> {
	/**
	 * Converts the given object into one Aerospike will be able to store natively. If the given object can already be stored
	 * as is, no conversion will happen.
	 * 
	 * @param obj can be {@literal null}.
	 * @return
	 */
	Object convertToAerospikeType(Object obj);

	/**
	 * Converts the given object into one Aerospike will be able to store natively but retains the type information in case
	 * the given {@link TypeInformation} differs from the given object type.
	 * 
	 * @param obj can be {@literal null}.
	 * @param typeInformation can be {@literal null}.
	 * @return
	 */
	Object convertToAerospikeType(Object obj, TypeInformation<?> typeInformation);

	/**
	 * Creates a {@link AerospikeWriteData} to refer to the given object.
	 * 
	 * @param object the object to create a {@link AerospikeWriteData} to link to. The object's type has to carry an id attribute.
	 * @param referingProperty the client-side property referring to the object which might carry additional metadata for
	 *          the {@link AerospikeWriteData} object to create. Can be {@literal null}.
	 * @return will never be {@literal null}.
	 */
	AerospikeWriteData toAerospikeData(Object object, AerospikePersistentProperty referingProperty);
}
