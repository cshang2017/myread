package org.apache.flink.python;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.description.Description;

/**
 * Configuration options for the Python API.
 */
@PublicEvolving
public class PythonOptions {

	/**
	 * The maximum number of elements to include in a bundle.
	 */
	public static final ConfigOption<Integer> MAX_BUNDLE_SIZE = ConfigOptions
		.key("python.fn-execution.bundle.size")
		.defaultValue(100000)
		.withDescription("The maximum number of elements to include in a bundle for Python " +
			"user-defined function execution. The elements are processed asynchronously. " +
			"One bundle of elements are processed before processing the next bundle of elements. " +
			"A larger value can improve the throughput, but at the cost of more memory usage and higher latency.");

	/**
	 * The maximum time to wait before finalising a bundle (in milliseconds).
	 */
	public static final ConfigOption<Long> MAX_BUNDLE_TIME_MILLS = ConfigOptions
		.key("python.fn-execution.bundle.time")
		.defaultValue(1000L)
		.withDescription("Sets the waiting timeout(in milliseconds) before processing a bundle for " +
			"Python user-defined function execution. The timeout defines how long the elements of a bundle will be " +
			"buffered before being processed. Lower timeouts lead to lower tail latencies, but may affect throughput.");

	/**
	 * The maximum number of elements to include in an arrow batch.
	 */
	public static final ConfigOption<Integer> MAX_ARROW_BATCH_SIZE = ConfigOptions
		.key("python.fn-execution.arrow.batch.size")
		.defaultValue(10000)
		.withDescription("The maximum number of elements to include in an arrow batch for Python " +
			"user-defined function execution. The arrow batch size should not exceed the " +
			"bundle size. Otherwise, the bundle size will be used as the arrow batch size.");

	/**
	 * The amount of memory to be allocated by the Python framework.
	 */
	public static final ConfigOption<String> PYTHON_FRAMEWORK_MEMORY_SIZE = ConfigOptions
		.key("python.fn-execution.framework.memory.size")
		.defaultValue("64mb")
		.withDescription("The amount of memory to be allocated by the Python framework. The sum " +
			"of the value of this configuration and \"python.fn-execution.buffer.memory.size\" " +
			"represents the total memory of a Python worker. The memory will be accounted as " +
			"managed memory if the actual memory allocated to an operator is no less than the " +
			"total memory of a Python worker. Otherwise, this configuration takes no effect.");

	/**
	 * The amount of memory to be allocated by the input/output buffer of a Python worker.
	 */
	public static final ConfigOption<String> PYTHON_DATA_BUFFER_MEMORY_SIZE = ConfigOptions
		.key("python.fn-execution.buffer.memory.size")
		.defaultValue("15mb")
		.withDescription("The amount of memory to be allocated by the input buffer and output " +
			"buffer of a Python worker. The memory will be accounted as managed memory if the " +
			"actual memory allocated to an operator is no less than the total memory of a Python " +
			"worker. Otherwise, this configuration takes no effect.");

	/**
	 * The configuration to enable or disable metric for Python execution.
	 */
	public static final ConfigOption<Boolean> PYTHON_METRIC_ENABLED = ConfigOptions
		.key("python.metric.enabled")
		.defaultValue(true)
		.withDescription("When it is false, metric for Python will be disabled. You can " +
			"disable the metric to achieve better performance at some circumstance.");

	public static final ConfigOption<String> PYTHON_FILES = ConfigOptions
		.key("python.files")
		.stringType()
		.noDefaultValue()
		.withDescription("Attach custom python files for job. These files will " +
			"be added to the PYTHONPATH of both the local client and the remote python UDF " +
			"worker. The standard python resource file suffixes such as .py/.egg/.zip or " +
			"directory are all supported. Comma (',') could be used as the separator to specify " +
			"multiple files. The option is equivalent to the command line option \"-pyfs\". ");

	public static final ConfigOption<String> PYTHON_REQUIREMENTS = ConfigOptions
		.key("python.requirements")
		.stringType()
		.noDefaultValue()
		.withDescription("Specify a requirements.txt file which defines the third-party " +
			"dependencies. These dependencies will be installed and added to the PYTHONPATH of " +
			"the python UDF worker. A directory which contains the installation packages of " +
			"these dependencies could be specified optionally. Use '#' as the separator if the " +
			"optional parameter exists. The option is equivalent to the command line option " +
			"\"-pyreq\".");

	public static final ConfigOption<String> PYTHON_ARCHIVES = ConfigOptions
		.key("python.archives")
		.stringType()
		.noDefaultValue()
		.withDescription("Add python archive files for job. The archive files will be extracted " +
			"to the working directory of python UDF worker. Currently only zip-format is " +
			"supported. For each archive file, a target directory is specified. If the target " +
			"directory name is specified, the archive file will be extracted to a name can " +
			"directory with the specified name. Otherwise, the archive file will be extracted to " +
			"a directory with the same name of the archive file. The files uploaded via this " +
			"option are accessible via relative path. '#' could be used as the separator of the " +
			"archive file path and the target directory name. Comma (',') could be used as the " +
			"separator to specify multiple archive files. This option can be used to upload the " +
			"virtual environment, the data files used in Python UDF. The data files could be " +
			"accessed in Python UDF, e.g.: f = open('data/data.txt', 'r'). The option is " +
			"equivalent to the command line option \"-pyarch\".");

	public static final ConfigOption<String> PYTHON_EXECUTABLE = ConfigOptions
		.key("python.executable")
		.stringType()
		.defaultValue("python")
		.withDescription("Specify the path of the python interpreter used to execute the python " +
			"UDF worker. The python UDF worker depends on Python 3.5+, Apache Beam " +
			"(version == 2.19.0), Pip (version >= 7.1.0) and SetupTools (version >= 37.0.0). " +
			"Please ensure that the specified environment meets the above requirements. The " +
			"option is equivalent to the command line option \"-pyexec\".");

	public static final ConfigOption<String> PYTHON_CLIENT_EXECUTABLE = ConfigOptions
		.key("python.client.executable")
		.stringType()
		.defaultValue("python")
		.withDescription(Description.builder()
			.text("The path of the Python interpreter used to launch the Python process when submitting the "
				+ "Python jobs via \"flink run\" or compiling the Java/Scala jobs containing Python UDFs. "
				+ "Equivalent to the environment variable PYFLINK_CLIENT_EXECUTABLE. "
				+ "The priority is as following: ").linebreak()
			.text("1. the configuration 'python.client.executable' defined in the source code;").linebreak()
			.text("2. the environment variable PYFLINK_CLIENT_EXECUTABLE;").linebreak()
			.text("3. the configuration 'python.client.executable' defined in flink-conf.yaml").build());

	/**
	 * Whether the memory used by the Python framework is managed memory.
	 */
	public static final ConfigOption<Boolean> USE_MANAGED_MEMORY = ConfigOptions
		.key("python.fn-execution.memory.managed")
		.defaultValue(false)
		.withDescription(String.format("If set, the Python worker will configure itself to use the " +
			"managed memory budget of the task slot. Otherwise, it will use the Off-Heap Memory " +
			"of the task slot. In this case, users should set the Task Off-Heap Memory using the " +
			"configuration key %s. For each Python worker, the required Task Off-Heap Memory " +
			"is the sum of the value of %s and %s.", TaskManagerOptions.TASK_OFF_HEAP_MEMORY.key(),
			PYTHON_FRAMEWORK_MEMORY_SIZE.key(), PYTHON_DATA_BUFFER_MEMORY_SIZE.key()));
}
