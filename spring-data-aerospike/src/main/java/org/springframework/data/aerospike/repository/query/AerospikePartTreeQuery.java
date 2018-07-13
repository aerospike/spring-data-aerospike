/**
 * 
 */
package org.springframework.data.aerospike.repository.query;

import java.lang.reflect.Constructor;

import org.springframework.beans.BeanUtils;
import org.springframework.data.aerospike.core.AerospikeOperations;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.keyvalue.core.IterableConverter;
import org.springframework.data.repository.query.EvaluationContextProvider;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.util.ClassUtils;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public class AerospikePartTreeQuery implements RepositoryQuery {
	
	private final EvaluationContextProvider evaluationContextProvider;
	private final QueryMethod queryMethod;
	private final AerospikeOperations aerospikeOperations;
	private final Class<? extends AbstractQueryCreator<?, ?>> queryCreator;

	private Query query;

	public AerospikePartTreeQuery(QueryMethod queryMethod, EvaluationContextProvider evalContextProvider,
			AerospikeOperations aerospikeOperations, Class<? extends AbstractQueryCreator<?, ?>> queryCreator) {

		this.queryMethod = queryMethod;
		this.aerospikeOperations = aerospikeOperations;
		this.evaluationContextProvider = evalContextProvider;
		this.queryCreator = queryCreator;
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#getQueryMethod()
	 */
	@Override
	public QueryMethod getQueryMethod() {
		return queryMethod;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#execute(java.lang.Object[])
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Object execute(Object[] parameters) {
		Query query = prepareQuery(parameters);

		if (queryMethod.isPageQuery() || queryMethod.isSliceQuery()) {
			Pageable page = (Pageable) parameters[queryMethod.getParameters().getPageableIndex()];
			query.setOffset(page.getOffset());
			query.setRows(page.getPageSize());

			Iterable<?> result = this.aerospikeOperations.find(query, queryMethod.getEntityInformation().getJavaType());
			long count = queryMethod.isSliceQuery() ? 0 : aerospikeOperations.count(query, queryMethod.getEntityInformation().getJavaType());

			return new PageImpl(IterableConverter.toList(result), page, count);
		} else if (queryMethod.isCollectionQuery()) {

			return this.aerospikeOperations.find(query, queryMethod.getEntityInformation().getJavaType());

		} else if (queryMethod.isQueryForEntity()) {

			Iterable<?> result = this.aerospikeOperations.find(query, queryMethod.getEntityInformation().getJavaType());
			return result.iterator().hasNext() ? result.iterator().next() : null;

		}

		throw new UnsupportedOperationException("Query method not supported.");
	}

	/**
	 * @param parameters
	 * @return
	 */
	private Query prepareQuery(Object[] parameters) {
		ParametersParameterAccessor accessor = new ParametersParameterAccessor(getQueryMethod().getParameters(), parameters);

		this.query = createQuery(accessor);

		AerospikeCriteria criteria = (AerospikeCriteria) query.getCritieria();
		Query q = new Query(criteria);

		if (accessor.getPageable() != null) {
			q.setOffset(accessor.getPageable().getOffset());
			q.setRows(accessor.getPageable().getPageSize());
		} else {
			q.setOffset(-1);
			q.setRows(-1);
		}

		if (accessor.getSort() != null) {
			q.setSort(accessor.getSort());
		} else {
			q.setSort(this.query.getSort());
		}

		if (q.getCritieria() instanceof SpelExpression) {
			EvaluationContext context = this.evaluationContextProvider.getEvaluationContext(getQueryMethod().getParameters(),
					parameters);
			((SpelExpression) q.getCritieria()).setEvaluationContext(context);
		}

		return q;
	}


	public Query createQuery(ParametersParameterAccessor accessor) {

		PartTree tree = new PartTree(getQueryMethod().getName(), getQueryMethod().getEntityInformation().getJavaType());

		Constructor<? extends AbstractQueryCreator<?, ?>> constructor = (Constructor<? extends AbstractQueryCreator<?, ?>>) ClassUtils
				.getConstructorIfAvailable(queryCreator, PartTree.class, ParameterAccessor.class);
		return (Query) BeanUtils.instantiateClass(constructor, tree, accessor).createQuery();
	}

}
