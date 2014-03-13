package org.springframework.batch.core.step;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Michael Minella
 *
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public class RestartInPriorStepTests {

	@Autowired
	private JobRepository jobRepository;

	@Autowired
	private JobExplorer jobExplorer;

	@Autowired
	private JobLauncher jobLauncher;

	@Autowired
	private Job job;

	@Test
	public void test() throws Exception {
		//
		// Run 1
		//
		JobExecution run1 = jobLauncher.run(job, new JobParameters());

		assertEquals(BatchStatus.STOPPED, run1.getStatus());
		assertEquals(2, run1.getStepExecutions().size());

		//
		// Run 2
		//
		JobExecution run2 = jobLauncher.run(job, new JobParameters());

		assertEquals(BatchStatus.COMPLETED, run2.getStatus());
		assertEquals(6, run2.getStepExecutions().size());
	}

	public static class DecidingTasklet implements Tasklet {

		@Override
		public RepeatStatus execute(StepContribution contribution,
				ChunkContext chunkContext) throws Exception {
			Map<String, Object> context = chunkContext.getStepContext().getJobExecutionContext();

			if(context.get("restart") != null) {
				contribution.setExitStatus(new ExitStatus("ES3"));
			} else {
				chunkContext.getStepContext().setAttribute("restart", true);
				contribution.setExitStatus(new ExitStatus("ES4"));
			}

			return RepeatStatus.FINISHED;
		}
	}

	public static class CompletionDecider implements JobExecutionDecider {

		private int count = 0;

		@Override
		public FlowExecutionStatus decide(JobExecution jobExecution,
				StepExecution stepExecution) {
			count++;

			if(count > 2) {
				return new FlowExecutionStatus("END");
			}
			else {
				return new FlowExecutionStatus("CONTINUE");
			}
		}
	}
}
