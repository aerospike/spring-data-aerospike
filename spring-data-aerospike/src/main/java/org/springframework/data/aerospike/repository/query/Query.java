/**
 * 
 */
package org.springframework.data.aerospike.repository.query;

import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.keyvalue.core.query.KeyValueQuery;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 * @param <T>
 *
 */
public class Query {

	private Sort sort;
	private int offset = -1;
	private int rows = -1;
	private CriteriaDefinition criteria;

	/**
	 * Creates new instance of {@link KeyValueQuery} with given criteria.
	 * 
	 * @param criteria can be {@literal null}.
	 */
	public Query(CriteriaDefinition criteria) {
		 this.criteria = criteria;
	}

	/**
	 * Creates new instance of {@link Query} with given {@link Sort}.
	 * 
	 * @param sort can be {@literal null}.
	 */
	public Query(Sort sort) {
		this.sort = sort;
	}

	/**
	 * Get the criteria object.
	 * 
	 * @return
	 */
	public CriteriaDefinition getCritieria() {
		return criteria;
	}

	/**
	 * Get {@link Sort}.
	 * 
	 * @return
	 */
	public Sort getSort() {
		return sort;
	}

	/**
	 * Number of elements to skip.
	 * 
	 * @return negative value if not set.
	 */
	public int getOffset() {
		return this.offset;
	}

	/**
	 * Number of elements to read.
	 * 
	 * @return negative value if not set.
	 */
	public int getRows() {
		return this.rows;
	}

	/**
	 * Set the number of elements to skip.
	 * 
	 * @param offset use negative value for none.
	 */
	public void setOffset(int offset) {
		this.offset = offset;
	}

	/**
	 * Set the number of elements to read.
	 * 
	 * @param offset use negative value for all.
	 */
	public void setRows(int rows) {
		this.rows = rows;
	}

	/**
	 * Set {@link Sort} to be applied.
	 * 
	 * @param sort
	 */
	public void setSort(Sort sort) {
		this.sort = sort;
	}

	/**
	 * Add given {@link Sort}.
	 * 
	 * @param sort {@literal null} {@link Sort} will be ignored.
	 * @return
	 */
	public Query orderBy(Sort sort) {
		if (sort == null) {
			return this;
		}

		if (this.sort != null) {
			this.sort.and(sort);
		}
		else {
			this.sort = sort;
		}
		return this;
	}

	/**
	 * @see Query#setOffset(int)
	 * @param offset
	 * @return
	 */
	public Query skip(int offset) {
		setOffset(offset);
		return this;
	}

	/**
	 * @see Query#setRows(int)
	 * @param rows
	 * @return
	 */
	public Query limit(int rows) {
		setRows(rows);
		return this;
	}

	/**
	 * @param sort
	 * @return
	 */
	public Query with(Sort sort) {
		if (sort == null) {
			return this;
		}

		for (Order order : sort) {
			if (order.isIgnoreCase()) {
				throw new IllegalArgumentException(String.format(
						"Given sort contained an Order for %s with ignore case! "
								+ "Aerospike does not support sorting ignoreing case currently!",
						order.getProperty()));
			}
		}

		if (this.sort == null) {
			this.sort = sort;
		}
		else {
			this.sort = this.sort.and(sort);
		}

		return this;
	}


//	/* (non-Javadoc)
//	 * @see java.lang.Object#toString()
//	 */
//	@Override
//	public String toString() {
//		StringBuilder res = new StringBuilder();
//		for (Qualifier qualifier : getQueryObject()) {
//			res.append(qualifier.luaFilterString());
//			res.append(',');
//		}
//		return res.toString();
//	}

}
