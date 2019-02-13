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

import java.util.Collection;
import javax.sql.DataSource;

import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.support.BatchTransactionManagerBeanFactoryPostProcessor;
import org.springframework.batch.core.configuration.support.MapJobRegistry;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.JobScope;
import org.springframework.batch.core.scope.StepScope;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportAware;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.Assert;

/**
 * Base {@code Configuration} class providing common structure for enabling and using Spring Batch. Customization is
 * available by implementing the {@link BatchConfigurer} interface. {@link BatchConfigurer}.
 * 
 * @author Dave Syer
 * @author Michael Minella
 * @author Mahmoud Ben Hassine
 * @since 2.2
 * @see EnableBatchProcessing
 */
@Configuration
@Import(ScopeConfiguration.class)
public abstract class AbstractBatchConfiguration implements ImportAware, ApplicationContextAware {

	@Autowired(required = false)
	private DataSource dataSource;

	private BatchConfigurer configurer;

	private GenericApplicationContext applicationContext;

	@Bean
	public BatchTransactionManagerBeanFactoryPostProcessor batchTransactionManagerBeanFactoryPostProcessor() {
		return new BatchTransactionManagerBeanFactoryPostProcessor();
	}

	@Bean
	public JobBuilderFactory jobBuilders() throws Exception {
		System.out.println(">> AbstractBatchConfiguration#jobBuilders was called");
		return new JobBuilderFactory(jobRepository());
	}

	@Bean
	public StepBuilderFactory stepBuilders() throws Exception {
		System.out.println(">> AbstractBatchConfiguration#stepBuilders was called");
		return new StepBuilderFactory(jobRepository(), transactionManager());
	}

	@Bean
	public abstract JobRepository jobRepository() throws Exception;

	@Bean
	public abstract JobLauncher jobLauncher() throws Exception;

	@Bean
	public abstract JobExplorer jobExplorer() throws Exception;

	@Bean
	public JobRegistry jobRegistry() throws Exception {
		System.out.println(">> AbstractBatchConfiguration#jobRegistry was called");
		return new MapJobRegistry();
	}

	public void setApplicationContext(ApplicationContext applicationContext) {
		System.out.println(">> AbstractBatchConfiguration#setApplicationContext was called");
		this.applicationContext = (GenericApplicationContext) applicationContext;
	}

//	protected void setTransactionManager(PlatformTransactionManager transactionManager) {
//		this.transactionManager = transactionManager;
//	}

//	@Bean
	public abstract PlatformTransactionManager transactionManager() throws Exception;

	@Override
	public void setImportMetadata(AnnotationMetadata importMetadata) {
		System.out.println(">> AbstractBatchConfiguration#setImportMetadata was called");
		AnnotationAttributes enabled = AnnotationAttributes.fromMap(importMetadata.getAnnotationAttributes(
				EnableBatchProcessing.class.getName(), false));
		Assert.notNull(enabled,
				"@EnableBatchProcessing is not present on importing class " + importMetadata.getClassName());
	}

	protected BatchConfigurer getConfigurer(Collection<BatchConfigurer> configurers) throws Exception {
		System.out.println(">> We got the configurer");
		if (this.configurer != null) {
			return this.configurer;
		}
		if (configurers == null || configurers.isEmpty()) {
			if (dataSource == null) {
				DefaultBatchConfigurer configurer = new DefaultBatchConfigurer();
				configurer.setApplicationContext(this.applicationContext);
				configurer.initialize();
				this.configurer = configurer;
				return configurer;
			} else {
				DefaultBatchConfigurer configurer = new DefaultBatchConfigurer(dataSource);
				configurer.setApplicationContext(this.applicationContext);
				configurer.initialize();
				this.configurer = configurer;
				return configurer;
			}
		}
		if (configurers.size() > 1) {
			throw new IllegalStateException(
					"To use a custom BatchConfigurer the context must contain precisely one, found "
							+ configurers.size());
		}
		this.configurer = configurers.iterator().next();
		return this.configurer;
	}

}

/**
 * Extract job/step scope configuration into a separate unit.
 * 
 * @author Dave Syer
 * 
 */
@Configuration
class ScopeConfiguration {

	@Bean
	public static StepScope stepScope() {
		StepScope stepScope = new StepScope();
		stepScope.setAutoProxy(false);
		return stepScope;
	}

	@Bean
	public static JobScope jobScope() {
		JobScope jobScope = new JobScope();
		jobScope.setAutoProxy(false);
		return jobScope;
	}

}
