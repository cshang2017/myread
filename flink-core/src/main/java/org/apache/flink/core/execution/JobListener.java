package org.apache.flink.core.execution;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobExecutionResult;

import javax.annotation.Nullable;

/**
 * A listener that is notified on specific job status changed, which should be firstly
 * registered by {@code #registerJobListener} of execution environments.
 *
 * <p>It is highly recommended NOT to perform any blocking operation inside
 * the callbacks. If you block the thread the invoker of environment execute methods
 * is possibly blocked.
 */
@PublicEvolving
public interface JobListener {

	/**
	 * Callback on job submission. This is called when {@code execute()} or {@code executeAsync()}
	 * is called.
	 *
	 * <p>Exactly one of the passed parameters is null, respectively for failure or success.
	 *
	 * @param jobClient a {@link JobClient} for the submitted Flink job
	 * @param throwable the cause if submission failed
	 */
	void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable);

	/**
	 * Callback on job execution finished, successfully or unsuccessfully. It is only called
	 * back when you call {@code execute()} instead of {@code executeAsync()} methods of execution
	 * environments.
	 *
	 * <p>Exactly one of the passed parameters is null, respectively for failure or success.
	 */
	void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable);

}
