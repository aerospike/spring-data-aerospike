/**
 *
 */
package org.springframework.data.aerospike.convert;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
@ReadingConverter
public class LongToBooleanConverter implements Converter<Long, Boolean> {

	/* (non-Javadoc)
	 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
	 */
	@Override
	public Boolean convert(Long source) {
		return source != 0L;
	}

}
