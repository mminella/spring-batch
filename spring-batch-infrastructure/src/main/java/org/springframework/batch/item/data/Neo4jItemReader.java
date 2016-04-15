/*
 * Copyright 2012 the original author or authors.
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

package org.springframework.batch.item.data;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.batch.item.ItemReader;
import org.springframework.data.neo4j.conversion.DefaultConverter;
import org.springframework.data.neo4j.conversion.Result;
import org.springframework.data.neo4j.conversion.ResultConverter;
import org.springframework.util.ClassUtils;

/**
 * <p>
 * Restartable {@link ItemReader} that reads objects from the graph database Neo4j
 * via a paging technique.
 * </p>
 *
 * <p>
 * It executes cypher queries built from the statement fragments provided to
 * retrieve the requested data.  The query is executed using paged requests of
 * a size specified in {@link #setPageSize(int)}.  Additional pages are requested
 * as needed when the {@link #read()} method is called.  On restart, the reader
 * will begin again at the same number item it left off at.
 * </p>
 *
 * <p>
 * Performance is dependent on your Neo4J configuration (embedded or remote) as
 * well as page size.  Setting a fairly large page size and using a commit
 * interval that matches the page size should provide better performance.
 * </p>
 *
 * <p>
 * This implementation is thread-safe between calls to
 * {@link #open(org.springframework.batch.item.ExecutionContext)}, however you
 * should set <code>saveState=false</code> if used in a multi-threaded
 * environment (no restart available).
 * </p>
 *
 * @author Michael Minella
 *
 */
public class Neo4jItemReader<T> extends AbstractNeo4jItemReader {

	protected Log logger = LogFactory.getLog(getClass());

	private ResultConverter<Map<String, Object>, T> resultConverter;

	public Neo4jItemReader() {
		setName(ClassUtils.getShortName(Neo4jItemReader.class));
	}

	/**
	 * Set the converter used to convert node to the targetType.  By
	 * default, {@link DefaultConverter} is used.
	 *
	 * @param resultConverter the converter to use.
	 */
	public void setResultConverter(ResultConverter<Map<String, Object>, T> resultConverter) {
		this.resultConverter = resultConverter;
	}

	@Override
	protected Iterator<T> doPageRead() {
		Result<Map<String, Object>> queryResults = getTemplate().query(
				generateLimitCypherQuery(), getParameterValues());

		if(queryResults != null) {
			if (resultConverter != null) {
				return queryResults.to(getTargetType(), resultConverter).iterator();
			}
			else {
				return queryResults.to(getTargetType()).iterator();
			}
		}
		else {
			return new ArrayList<T>().iterator();
		}
	}
}
