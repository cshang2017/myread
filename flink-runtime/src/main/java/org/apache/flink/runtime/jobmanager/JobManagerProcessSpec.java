package org.apache.flink.runtime.jobmanager;

import org.apache.flink.runtime.util.config.memory.CommonProcessMemorySpec;
import org.apache.flink.runtime.util.config.memory.JvmMetaspaceAndOverhead;
import org.apache.flink.runtime.util.config.memory.jobmanager.JobManagerFlinkMemory;

/**
 * Describe the specifics of different resource dimensions of the JobManager process.
 *
 * <p>A JobManager's memory consists of the following components:
 * <ul>
 *     <li>JVM Heap Memory</li>
 *     <li>Off-heap Memory</li>
 *     <li>JVM Metaspace</li>
 *     <li>JVM Overhead</li>
 * </ul>
 * We use Total Process Memory to refer to all the memory components, while Total Flink Memory refering to all
 * the components except JVM Metaspace and JVM Overhead.
 *
 * <p>The relationships of JobManager memory components are shown below.
 * <pre>
 *               ┌ ─ ─ Total Process Memory  ─ ─ ┐
 *                ┌ ─ ─ Total Flink Memory  ─ ─ ┐
 *               │ ┌───────────────────────────┐ │
 *  On-Heap ----- ││      JVM Heap Memory      ││
 *               │ └───────────────────────────┘ │
 *               │ ┌───────────────────────────┐ │
 *            ┌─  ││       Off-heap Memory     ││
 *            │  │ └───────────────────────────┘ │
 *            │   └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
 *            │  │┌─────────────────────────────┐│
 *  Off-Heap ─|   │        JVM Metaspace        │
 *            │  │└─────────────────────────────┘│
 *            │   ┌─────────────────────────────┐
 *            └─ ││        JVM Overhead         ││
 *                └─────────────────────────────┘
 *               └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
 * </pre>
 */
public class JobManagerProcessSpec extends CommonProcessMemorySpec<JobManagerFlinkMemory> {

	JobManagerProcessSpec(JobManagerFlinkMemory flinkMemory, JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead) {
		super(flinkMemory, jvmMetaspaceAndOverhead);
	}

	@Override
	public String toString() {
		return "JobManagerProcessSpec {" +
			"jvmHeapSize=" + getJvmHeapMemorySize().toHumanReadableString() + ", " +
			"offHeapSize=" + getJvmDirectMemorySize().toHumanReadableString() + ", " +
			"jvmMetaspaceSize=" + getJvmMetaspaceSize().toHumanReadableString() + ", " +
			"jvmOverheadSize=" + getJvmOverheadSize().toHumanReadableString() + '}';
	}
}
