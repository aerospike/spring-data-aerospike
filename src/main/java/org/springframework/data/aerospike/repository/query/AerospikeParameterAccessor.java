/**
 * 
 */
package org.springframework.data.aerospike.repository.query;

import org.springframework.data.repository.query.ParameterAccessor;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public interface AerospikeParameterAccessor extends ParameterAccessor {

	/**
	 * Returns the raw parameter values of the underlying query method.
	 * 
	 * @return
	 * @since 1.8
	 */
	Object[] getValues();
}
