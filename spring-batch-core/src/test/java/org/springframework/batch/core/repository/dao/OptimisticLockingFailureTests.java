/*
 * Copyright 2014 the original author or authors.
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

package org.springframework.batch.core.repository.dao;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

import static org.junit.Assert.assertTrue;

@Ignore
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public class OptimisticLockingFailureTests {
	@Autowired
	private Job job;

	@Autowired
	private JobLauncher jobLauncher;

	@Autowired
	private JobOperator jobOperator;

	@Test
	public void testAsyncStopOfStartingJob() throws Exception {
		JobExecution jobExecution = jobLauncher.run(job, new JobParametersBuilder()
				.addLong("test", 1L)
				.toJobParameters());

		jobOperator.stop(jobExecution.getId());

		while(jobExecution.isRunning()) {
			// wait for async launched job to complete execution
		}

		int numStepExecutions = jobExecution.getStepExecutions().size();
		StepExecution stepExecution = jobExecution.getStepExecutions().iterator().next();
		String stepName = stepExecution.getStepName();
		BatchStatus stepExecutionStatus = stepExecution.getStatus();
		BatchStatus jobExecutionStatus = jobExecution.getStatus();

		assertTrue("Should only be one StepExecution but got: " + numStepExecutions, numStepExecutions == 1);
		assertTrue("Step name for execution should be step1 but got: " + stepName, "step1".equals(stepName));
		assertTrue("Step execution status should be STOPPED but got: " + stepExecutionStatus, stepExecutionStatus.equals(BatchStatus.STOPPED));
		assertTrue("Job execution status should be STOPPED but got:" + jobExecutionStatus, jobExecutionStatus.equals(BatchStatus.STOPPED));

		JobExecution restartJobExecution = jobLauncher.run(job, new JobParametersBuilder()
				.addLong("test", 1L)
				.toJobParameters());

		while(restartJobExecution.isRunning()) {
			// wait for async launched job to complete execution
		}

		int restartNumStepExecutions = restartJobExecution.getStepExecutions().size();
		assertTrue("Should be two StepExecution's on restart but got: " + restartNumStepExecutions, restartNumStepExecutions == 2);

		for(StepExecution restartStepExecution : restartJobExecution.getStepExecutions()) {
			BatchStatus restartStepExecutionStatus = restartStepExecution.getStatus();

			assertTrue("Step execution status should be COMPLETED but got: " + restartStepExecutionStatus,
					restartStepExecutionStatus.equals(BatchStatus.COMPLETED));
		}

		BatchStatus restartJobExecutionStatus = restartJobExecution.getStatus();
		assertTrue("Job execution status should be COMPLETED but got:" + restartJobExecutionStatus,
				restartJobExecutionStatus.equals(BatchStatus.COMPLETED));
	}

	public static class Writer implements ItemWriter<String> {
		@Override
		public void write(List<? extends String> items) throws Exception {
			for(String item : items) {
				System.out.println(item);
			}
		}
	}
}
