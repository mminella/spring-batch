/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.batch.core.configuration.support;

import javax.sql.DataSource;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * @author Michael Minella
 */
public class BatchTransactionManagerBeanFactoryPostProcessor implements BeanFactoryPostProcessor, ApplicationContextAware {

	private GenericApplicationContext applicationContext;

	@Override
	public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
		System.out.println(">> POST PROCESSING BEANS!!!");
		if(!hasBeansOfType(beanFactory, PlatformTransactionManager.class)) {
			System.out.println(">> POST PROCESSING BEANS!!! NO TRANSACTION MANAGERS");
			//TODO: JpaTransactionManager

			if(hasBeansOfType(beanFactory, DataSource.class)) {
				System.out.println(">> POST PROCESSING BEANS!!! NO TRANSACTION MANAGERS BUT WE HAVE A DATASOURCE");
				DataSource dataSource = beanFactory.getBean(DataSource.class);

				final DataSourceTransactionManager transactionManager =
						new DataSourceTransactionManager(dataSource);

				this.applicationContext.registerBean("transactionManager", DataSourceTransactionManager.class, () -> transactionManager);
			}

			//TODO: Resourceless TransactionManager
		}
	}

	private boolean hasBeansOfType(ConfigurableListableBeanFactory beanFactory, Class<?> clazz) {
		return beanFactory.getBeanNamesForType(clazz).length > 0;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		System.out.println(">> POST PROCESSING BEANS!!! CONTEXT SET");
		this.applicationContext = (GenericApplicationContext) applicationContext;
	}
}
