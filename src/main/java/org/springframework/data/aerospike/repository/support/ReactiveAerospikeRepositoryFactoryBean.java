/*
 * Copyright 2012-2019 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.repository.support;

import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;

/**
 * @author Igor Ermolenko
 */
public class ReactiveAerospikeRepositoryFactoryBean<T extends Repository<S, ID>, S, ID>
    extends BaseAerospikeRepositoryFactoryBean<T, S, ID> {

    private ReactiveAerospikeTemplate template;

    public ReactiveAerospikeRepositoryFactoryBean(Class<? extends T> repositoryInterface) {
        super(repositoryInterface);
    }

    public void setTemplate(ReactiveAerospikeTemplate reactiveTemplate) {
        this.template = reactiveTemplate;
    }

    @Override
    protected RepositoryFactorySupport createRepositoryFactory() {
        return new ReactiveAerospikeRepositoryFactory(this.template, this.queryCreator);
    }
}
