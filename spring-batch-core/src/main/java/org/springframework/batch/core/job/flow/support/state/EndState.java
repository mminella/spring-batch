/*
 * Copyright 2006-2013 the original author or authors.
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

package org.springframework.batch.core.job.flow.support.state;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.FlowExecutor;
import org.springframework.batch.core.job.flow.State;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ExecutionContext;

/**
 * {@link State} implementation for ending a job if it is in progress and
 * continuing if just starting.
 *
 * @author Dave Syer
 * @since 2.0
 */
public class EndState extends AbstractState {

	private final FlowExecutionStatus status;

	private final boolean abandon;

	private final String code;

	private JobRepository jobRepository;

	private String restart;

	/**
	 * @param status The {@link FlowExecutionStatus} to end with
	 * @param name The name of the state
	 */
	public EndState(FlowExecutionStatus status, String name) {
		this(status, status.getName(), name);
	}

	/**
	 * @param status The {@link FlowExecutionStatus} to end with
	 * @param name The name of the state
	 */
	public EndState(FlowExecutionStatus status, String code, String name) {
		this(status, code, name, false);
	}

	/**
	 * @param status The {@link FlowExecutionStatus} to end with
	 * @param name The name of the state
	 * @param abandon flag to indicate that previous step execution can be
	 * marked as abandoned (if there is one)
	 *
	 */
	public EndState(FlowExecutionStatus status, String code, String name, boolean abandon) {
		super(name);
		this.status = status;
		this.code = code;
		this.abandon = abandon;
	}

	/**
	 * @param status The {@link FlowExecutionStatus} to end with
	 * @param name The name of the state
	 * @param abandon flag to indicate that previous step execution can be
	 * marked as abandoned (if there is one)
	 *
	 */
	public EndState(FlowExecutionStatus status, String code, String name, boolean abandon, JobRepository jobRepository) {
		super(name);
		this.status = status;
		this.code = code;
		this.abandon = abandon;
		this.jobRepository = jobRepository;
	}

	/**
	 * Return the {@link FlowExecutionStatus} stored.
	 *
	 * @see State#handle(FlowExecutor)
	 */
	@Override
	public FlowExecutionStatus handle(FlowExecutor executor) throws Exception {

		synchronized (executor) {

			// Special case. If the last step execution could not complete we
			// are in an unknown state (possibly unrecoverable).
			StepExecution stepExecution = executor.getStepExecution();
			if (stepExecution != null && executor.getStepExecution().getStatus() == BatchStatus.UNKNOWN) {
				return FlowExecutionStatus.UNKNOWN;
			}

			if (status.isStop()) {
				//TODO: Refactor this to not need to look back to the previous state (step).
				JobExecution jobExecution = stepExecution.getJobExecution();
				ExecutionContext executionContext = jobExecution.getExecutionContext();
				executionContext.put("batch.restartStep", restart);
				executionContext.put("batch.stoppedStep", stepExecution.getStepName());
				jobRepository.updateExecutionContext(jobExecution);

				if (!executor.isRestart()) {
					/*
					 * If there are step executions, then we are not at the
					 * beginning of a restart.
					 */
					if (abandon) {
						/*
						 * Only if instructed to do so, upgrade the status of
						 * last step execution so it is not replayed on a
						 * restart...
						 */
						executor.abandonStepExecution();
					}
				}
				else {
					/*
					 * If we are a stop state and we got this far then it must
					 * be a restart, so return COMPLETED.
					 */
					return FlowExecutionStatus.COMPLETED;
				}
			}

			executor.addExitStatus(code);
			return status;

		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.springframework.batch.core.job.flow.State#isEndState()
	 */
	@Override
	public boolean isEndState() {
		return !status.isStop();
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return super.toString() + " status=[" + status + "]";
	}
}
