/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.batch.item.database.builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseFactory;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import static junit.framework.TestCase.assertEquals;

/**
 * @author Michael Minella
 */
public class JdbcBatchItemWriterBuilderTests {

	private DataSource dataSource;

	private ConfigurableApplicationContext context;

	@Before
	public void setUp() {
		this.context = new AnnotationConfigApplicationContext(TestDataSourceConfiguration.class);
		this.dataSource = (DataSource) context.getBean("dataSource");
	}

	@After
	public void tearDown() {
		if(this.context != null) {
			this.context.close();
		}
	}

	@Test
	public void testBasicMap() throws Exception {
		JdbcBatchItemWriter<Map<String, Object>> writer = new JdbcBatchItemWriterBuilder<Map<String, Object>>()
				.columnMapped()
				.dataSource(this.dataSource)
				.sql("INSERT INTO FOO (first, second, third) VALUES (:first, :second, :third)")
				.build();

		writer.afterPropertiesSet();

		List<Map<String, Object>> items = new ArrayList<>(3);

		Map<String, Object> item = new HashMap<>(3);
		item.put("first", 1);
		item.put("second", "two");
		item.put("third", "three");
		items.add(item);

		item = new HashMap<>(3);
		item.put("first", 4);
		item.put("second", "five");
		item.put("third", "six");
		items.add(item);

		item = new HashMap<>(3);
		item.put("first", 7);
		item.put("second", "eight");
		item.put("third", "nine");
		items.add(item);
		writer.write(items);

		verifyRow(1, "two", "three");
		verifyRow(4, "five", "six");
		verifyRow(7, "eight", "nine");
	}

	@Test
	public void testBasicPojo() throws Exception {
		JdbcBatchItemWriter<Foo> writer = new JdbcBatchItemWriterBuilder<Foo>()
				.beanMapped()
				.dataSource(this.dataSource)
				.sql("INSERT INTO FOO (first, second, third) VALUES (:first, :second, :third)")
				.build();

		writer.afterPropertiesSet();

		List<Foo> items = new ArrayList<>(3);

		items.add(new Foo(1, "two", "three"));
		items.add(new Foo(4, "five", "six"));
		items.add(new Foo(7, "eight", "nine"));

		writer.write(items);

		verifyRow(1, "two", "three");
		verifyRow(4, "five", "six");
		verifyRow(7, "eight", "nine");
	}

	private void verifyRow(int i, String i1, String nine) {
		JdbcOperations template = new JdbcTemplate(this.dataSource);

		assertEquals(1, (int) template.queryForObject(
				"select count(*) from foo where first = ? and second = ? and third = ?",
				new Object[] {i, i1, nine}, Integer.class));
	}

	public static class Foo {
		private int first;
		private String second;
		private String third;

		public Foo(int first, String second, String third) {
			this.first = first;
			this.second = second;
			this.third = third;
		}

		public int getFirst() {
			return first;
		}

		public void setFirst(int first) {
			this.first = first;
		}

		public String getSecond() {
			return second;
		}

		public void setSecond(String second) {
			this.second = second;
		}

		public String getThird() {
			return third;
		}

		public void setThird(String third) {
			this.third = third;
		}
	}

	@Configuration
	public static class TestDataSourceConfiguration {

		private static final String CREATE_SQL = "CREATE TABLE FOO  (\n" +
				"\tID BIGINT IDENTITY NOT NULL PRIMARY KEY ,\n" +
				"\tFIRST BIGINT ,\n" +
				"\tSECOND VARCHAR(5) NOT NULL,\n" +
				"\tTHIRD VARCHAR(5) NOT NULL) ;";

		@Bean
		public DataSource dataSource() {
			return new EmbeddedDatabaseFactory().getDatabase();
		}

		@Bean
		public DataSourceInitializer initializer(DataSource dataSource) {
			DataSourceInitializer dataSourceInitializer = new DataSourceInitializer();
			dataSourceInitializer.setDataSource(dataSource);

			Resource create = new ByteArrayResource(CREATE_SQL.getBytes());
			dataSourceInitializer.setDatabasePopulator(new ResourceDatabasePopulator(create));

			return dataSourceInitializer;
		}
	}
}
