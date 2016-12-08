/*
 * Copyright 2006-2007 the original author or authors.
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

package test.jdbc.datasource;

import java.io.IOException;
import java.util.List;
import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Wrapper for a {@link DataSource} that can run scripts on start up and shut
 * down.  Us as a bean definition <br><br>
 * 
 * Run this class to initialize a database in a running server process.
 * Make sure the server is running first by launching the "hsql-server" from the
 * <code>hsql.server</code> project. Then you can right click in Eclipse and
 * Run As -&gt; Java Application. Do the same any time you want to wipe the
 * database and start again.
 * 
 * @author Dave Syer
 * 
 */
public class DataSourceInitializer implements InitializingBean, DisposableBean {

	private static final Log logger = LogFactory.getLog(DataSourceInitializer.class);

	private Resource[] initScripts;

	private Resource[] destroyScripts;

	private DataSource dataSource;

	private boolean ignoreFailedDrop = true;

	private boolean initialized = false;

	/**
	 * Main method as convenient entry point.
	 * 
	 * @param args
	 */
	@SuppressWarnings("resource")
	public static void main(String... args) {
		new ClassPathXmlApplicationContext(ClassUtils.addResourcePathToPackagePath(DataSourceInitializer.class,
				DataSourceInitializer.class.getSimpleName() + "-context.xml"));
	}

	/**
	 * @throws Throwable
	 * @see java.lang.Object#finalize()
	 */
    @Override
	protected void finalize() throws Throwable {
		logger.debug("finalize called for " + dataSource);
		super.finalize();
		initialized = false;
	}

    @Override
	public void destroy() {
		logger.info("destroy called for " + dataSource);
		doDestroy();
	}

	public void doDestroy() {
		if (destroyScripts==null) return;
		for (int i = 0; i < destroyScripts.length; i++) {
			Resource destroyScript = destroyScripts[i];
			try {
				doExecuteScript(destroyScript);
			}
			catch (Exception e) {
				if (logger.isDebugEnabled()) {
					logger.debug("Could not execute destroy script [" + destroyScript + "]", e);
				}
				else {
					logger.warn("Could not execute destroy script [" + destroyScript + "]");
				}
			}
		}
	}

    @Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(dataSource);
		initialize();
	}

	private void initialize() {
		System.out.println("initializing the database");
		if (!initialized) {
			doDestroy();
			System.out.println("doDestroy complete");
			if (initScripts != null) {
				for (int i = 0; i < initScripts.length; i++) {
					Resource initScript = initScripts[i];
					doExecuteScript(initScript);
				}
			}
			initialized = true;
		}
	}

	private void doExecuteScript(final Resource scriptResource) {
		if (scriptResource == null || !scriptResource.exists())
			return;

		try {
			System.out.println("About to run script -> " + scriptResource.getURL());
		}
		catch (IOException e) {
			throw new RuntimeException("logging message failed: " + e.getMessage(), e);
		}

		final JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

		TransactionTemplate transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
		transactionTemplate.execute(new TransactionCallback<Void>() {

            @Override
			@SuppressWarnings("unchecked")
			public Void doInTransaction(TransactionStatus status) {
				String[] scripts;
				try {
					scripts = StringUtils.delimitedListToStringArray(stripComments(IOUtils.readLines(scriptResource
							.getInputStream())), ";");
				}
				catch (IOException e) {
					throw new BeanInitializationException("Cannot load script from [" + scriptResource + "]", e);
				}
				for (int i = 0; i < scripts.length; i++) {
					String script = scripts[i].trim();
					if (StringUtils.hasText(script)) {
						try {
							jdbcTemplate.execute(script);
						}
						catch (DataAccessException e) {
							try {
								System.out.println(">> script " + scriptResource.getURL() + " was not executed successfully");
							}
							catch (IOException e1) {
								System.out.println("This isn't good: " + e1.getMessage());

								throw new RuntimeException(e1);
							}
							if (ignoreFailedDrop && script.toLowerCase().startsWith("drop")) {
								logger.debug("DROP script failed (ignoring): " + script);
							}
							else {
								throw e;
							}
						}
					}
				}

				System.out.println(">> Scripts have been run");

//				try {
//					Connection conn = jdbcTemplate.getDataSource().getConnection();
//					DatabaseMetaData dbmd = conn.getMetaData();
//					String[] types = {"TABLE"};
//					ResultSet rs = dbmd.getTables(null, null, "%", types);
//					while (rs.next()) {
//						System.out.println(rs.getString("TABLE_NAME"));
//					}
//				}
//				catch (SQLException e) {
//					System.out.println(">>  An error was thrown when trying to list the tables: " + e.getMessage());
//				}

				return null;
			}

		});

	}

	private String stripComments(List<String> list) {
		StringBuilder buffer = new StringBuilder();
		for (String line : list) {
			if (!line.startsWith("//") && !line.startsWith("--")) {
				buffer.append(line).append("\n");
			}
		}
		return buffer.toString();
	}

	public void setInitScripts(Resource[] initScripts) {
		this.initScripts = initScripts;
	}

	public void setDestroyScripts(Resource[] destroyScripts) {
		this.destroyScripts = destroyScripts;
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public void setIgnoreFailedDrop(boolean ignoreFailedDrop) {
		this.ignoreFailedDrop = ignoreFailedDrop;
	}

}
