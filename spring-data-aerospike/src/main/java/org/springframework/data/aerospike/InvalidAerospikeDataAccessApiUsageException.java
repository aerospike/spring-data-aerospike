/**
 * 
 */
package org.springframework.data.aerospike;

import org.springframework.dao.InvalidDataAccessApiUsageException;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public class InvalidAerospikeDataAccessApiUsageException extends InvalidDataAccessApiUsageException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5449729113380096389L;

	public InvalidAerospikeDataAccessApiUsageException(String msg) {
		super(msg);
	}

	public InvalidAerospikeDataAccessApiUsageException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
