/**
 * 
 */
package org.springframework.data.aerospike.repository.support;
import java.io.Serializable;

import org.springframework.data.aerospike.core.AerospikeOperations;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public class AerospikeRepositoryFactoryBean<T extends Repository <S, ID>, S, ID extends Serializable> extends
RepositoryFactoryBeanSupport<T, S, ID> {
	
	private AerospikeOperations operations;
	private Class<? extends AbstractQueryCreator<?, ?>> queryCreator;
	
	public AerospikeRepositoryFactoryBean(Class<? extends T> repositoryInterface) {
		super(repositoryInterface);
	}

	@Override
	public void setMappingContext(MappingContext<?, ?> mappingContext) {
		super.setMappingContext(mappingContext);
	}

	public void setOperations(AerospikeOperations operations) {
		this.operations = operations;
	}

	public void setQueryCreator(
			Class<? extends AbstractQueryCreator<?, ?>> queryCreator) {
		this.queryCreator = queryCreator;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport#createRepositoryFactory()
	 */
	@Override
	protected RepositoryFactorySupport createRepositoryFactory() {
		// TODO Auto-generated method stub
		return new AerospikeRepositoryFactory(this.operations, this.queryCreator);
	}
	
	public void setKeyValueOperations(AerospikeOperations operations) {
		this.operations = operations;
	}
	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() {
		super.afterPropertiesSet();
	}

}
