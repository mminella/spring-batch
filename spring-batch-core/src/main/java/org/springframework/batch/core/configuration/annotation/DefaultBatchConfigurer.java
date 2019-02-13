/*
 * Copyright 2012-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.batch.core.configuration.annotation;

import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.target.AbstractLazyCreationTargetSource;
import org.springframework.batch.core.configuration.BatchConfigurationException;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.JobExplorerFactoryBean;
import org.springframework.batch.core.explore.support.MapJobExplorerFactoryBean;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

@Component
public class DefaultBatchConfigurer implements BatchConfigurer, ApplicationContextAware {
	private static final Log logger = LogFactory.getLog(DefaultBatchConfigurer.class);

	private DataSource dataSource;
	private PlatformTransactionManager transactionManager;
	private JobRepository jobRepository;
	private JobLauncher jobLauncher;
	private JobExplorer jobExplorer;
	private GenericApplicationContext applicationContext;

	/**
	 * Sets the dataSource.  If the {@link DataSource} has been set once, all future
	 * values are passed are ignored (to prevent {@code}@Autowired{@code} from overwriting
	 * the value).
	 *
	 * @param dataSource The data source to use
	 */
	@Autowired(required = false)
	public void setDataSource(DataSource dataSource) {
		System.out.println(">> setDataSource was called");
		if(this.dataSource == null) {
			this.dataSource = dataSource;
		}

//		if(getTransactionManager() == null) {
//			final DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(this.dataSource);
//			this.transactionManager = transactionManager;
//			this.applicationContext.registerBean("transactionManager", DataSourceTransactionManager.class, () -> transactionManager);
//		}
//		if(getTransactionManager() == null) {
//			logger.warn("No transaction manager was provided, using a DataSourceTransactionManager");
//			this.transactionManager = new DataSourceTransactionManager(this.dataSource);
//		}
	}

	@Autowired
	public void setTransactionManager(PlatformTransactionManager transactionManager) {
		this.transactionManager = transactionManager;
	}

	protected DefaultBatchConfigurer() {		System.out.println(">> DefaultBatchConfigurer() was called");
	}

	public DefaultBatchConfigurer(DataSource dataSource) {
		System.out.println(">> DefaultBatchConfigurer(DataSource) was called");
		setDataSource(dataSource);
	}

	@Override
	public JobRepository getJobRepository() {
		System.out.println(">> DefaultBatchConfigurer#getJobRepository was called");
		return jobRepository;
	}

	@Override
	public PlatformTransactionManager getTransactionManager() {
		System.out.println(">> DefaultBatchConfigurer#getTransactionManager was called");
		return transactionManager;
	}

	@Override
	public JobLauncher getJobLauncher() {
		System.out.println(">> DefaultBatchConfigurer#getJobLauncher was called");
		return jobLauncher;
	}

	@Override
	public JobExplorer getJobExplorer() {
		System.out.println(">> DefaultBatchConfigurer#getJobExplorer was called");
		return jobExplorer;
	}

	@PostConstruct
	public void initialize() {
		System.out.println(">> defaultBatchConfigurer.intialize was called");
		try {
			if(dataSource == null) {
				logger.warn("No datasource was provided...using a Map based JobRepository");

//				if(getTransactionManager() == null) {
//					logger.warn("No transaction manager was provided, using a ResourcelessTransactionManager");
//					final ResourcelessTransactionManager transactionManager = new ResourcelessTransactionManager();
//					this.transactionManager = transactionManager;
//					this.applicationContext.registerBean("transactionManager", ResourcelessTransactionManager.class,
//							() -> transactionManager);
//				}

				MapJobRepositoryFactoryBean jobRepositoryFactory = new MapJobRepositoryFactoryBean(getTransactionManager());
				jobRepositoryFactory.afterPropertiesSet();
				this.jobRepository = jobRepositoryFactory.getObject();

				MapJobExplorerFactoryBean jobExplorerFactory = new MapJobExplorerFactoryBean(jobRepositoryFactory);
				jobExplorerFactory.afterPropertiesSet();
				this.jobExplorer = jobExplorerFactory.getObject();
			} else {
				this.jobRepository = createJobRepository();
				this.jobExplorer = createJobExplorer();
			}

			this.jobLauncher = createJobLauncher();
		} catch (Exception e) {
			throw new BatchConfigurationException(e);
		}
	}

	protected JobLauncher createJobLauncher() throws Exception {
		System.out.println(">> DefaultBatchConfigurer#createJobLauncher was called");
		SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
		jobLauncher.setJobRepository(jobRepository);
		jobLauncher.afterPropertiesSet();
		return jobLauncher;
	}

	protected JobRepository createJobRepository() throws Exception {
		System.out.println(">> createJobRepository was called");
//		if(this.dataSource != null) {
//			if(getTransactionManager() == null) {
//				final DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(this.dataSource);
//				this.transactionManager = transactionManager;
//				this.applicationContext.registerBean("transactionManager", DataSourceTransactionManager.class, () -> transactionManager);
//			}
//		}

		JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
		factory.setDataSource(dataSource);
		factory.setTransactionManager(getTransactionManager());
		factory.afterPropertiesSet();
		return factory.getObject();
	}

	protected JobExplorer createJobExplorer() throws Exception {
		System.out.println(">> DefaultBatchConfigurer#createJobExplorer was called");
		JobExplorerFactoryBean jobExplorerFactoryBean = new JobExplorerFactoryBean();
		jobExplorerFactoryBean.setDataSource(this.dataSource);
		jobExplorerFactoryBean.afterPropertiesSet();
		return jobExplorerFactoryBean.getObject();
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		System.out.println(">> setApplicationContext was called");
		this.applicationContext = (GenericApplicationContext) applicationContext;
	}

	private <T> T createLazyProxy(AtomicReference<T> reference, Class<T> type) {
		ProxyFactory factory = new ProxyFactory();
		factory.setTargetSource(new ReferenceTargetSource<>(reference));
		factory.addAdvice(new PassthruAdvice());
		factory.setInterfaces(new Class<?>[] { type });
		@SuppressWarnings("unchecked")
		T proxy = (T) factory.getProxy();
		return proxy;
	}

	private class PassthruAdvice implements MethodInterceptor {

		@Override
		public Object invoke(MethodInvocation invocation) throws Throwable {
			return invocation.proceed();
		}

	}

	private class ReferenceTargetSource<T> extends AbstractLazyCreationTargetSource {

		private AtomicReference<T> reference;

		public ReferenceTargetSource(AtomicReference<T> reference) {
			this.reference = reference;
		}

		@Override
		protected Object createObject() throws Exception {
			initialize();
			return reference.get();
		}
	}

}
