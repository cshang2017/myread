package org.apache.flink.api.common.io;


import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.InputSplitSource;

/**
 * The base interface for data sources that produces records.
 * <p>
 * The input format handles the following:
 * <ul>
 *   <li>It describes how the input is split into splits that can be processed in parallel.</li>
 *   <li>It describes how to read records from the input split.</li>
 *   <li>It describes how to gather basic statistics from the input.</li> 
 * </ul>
 * <p>
 * The life cycle of an input format is the following:
 * <ol>
 *   <li>After being instantiated (parameterless), it is configured with a {@link Configuration} object. 
 *       Basic fields are read from the configuration, such as a file path, if the format describes
 *       files as input.</li>
 *   <li>Optionally: It is called by the compiler to produce basic statistics about the input.</li>
 *   <li>It is called to create the input splits.</li>
 *   <li>Each parallel input task creates an instance, configures it and opens it for a specific split.</li>
 *   <li>All records are read from the input</li>
 *   <li>The input format is closed</li>
 * </ol>
 * <p>
 * IMPORTANT NOTE: Input formats must be written such that an instance can be opened again after it was closed. That
 * is due to the fact that the input format is used for potentially multiple splits. After a split is done, the
 * format's close function is invoked and, if another split is available, the open function is invoked afterwards for
 * the next split.
 *  
 * @see InputSplit
 * @see BaseStatistics
 * 
 * @param <OT> The type of the produced records.
 * @param <T> The type of input split.
 */
@Public
public interface InputFormat<OT, T extends InputSplit> extends InputSplitSource<T>, Serializable {
	
	/**
	 * Configures this input format. Since input formats are instantiated generically and hence parameterless, 
	 * this method is the place where the input formats set their basic fields based on configuration values.
	 * <p>
	 * This method is always called first on a newly instantiated input format. 
	 *  
	 * @param parameters The configuration with all parameters (note: not the Flink config but the TaskConfig).
	 */
	void configure(Configuration parameters);
	
	/**
	 * Gets the basic statistics from the input described by this format. If the input format does not know how
	 * to create those statistics, it may return null.
	 * This method optionally gets a cached version of the statistics. The input format may examine them and decide
	 * whether it directly returns them without spending effort to re-gather the statistics.
	 * <p>
	 * When this method is called, the input format is guaranteed to be configured.
	 * 
	 * @param cachedStatistics The statistics that were cached. May be null.
	 * @return The base statistics for the input, or null, if not available.
	 */
	BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException;

	// --------------------------------------------------------------------------------------------

	@Override
	T[] createInputSplits(int minNumSplits) throws IOException;

	@Override
	InputSplitAssigner getInputSplitAssigner(T[] inputSplits);
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Opens a parallel instance of the input format to work on a split.
	 * <p>
	 * When this method is called, the input format it guaranteed to be configured.
	 * 
	 * @param split The split to be opened.
	 * @throws IOException Thrown, if the spit could not be opened due to an I/O problem.
	 */
	void open(T split) throws IOException;
	
	/**
	 * Method used to check if the end of the input is reached.
	 * <p>
	 * When this method is called, the input format it guaranteed to be opened.
	 * 
	 * @return True if the end is reached, otherwise false.
	 * @throws IOException Thrown, if an I/O error occurred.
	 */
	boolean reachedEnd() throws IOException;
	
	/**
	 * Reads the next record from the input.
	 * <p>
	 * When this method is called, the input format it guaranteed to be opened.
	 * 
	 * @param reuse Object that may be reused.
	 * @return Read record.
	 *         
	 * @throws IOException Thrown, if an I/O error occurred.
	 */
	OT nextRecord(OT reuse) throws IOException;
	
	/**
	 * Method that marks the end of the life-cycle of an input split. Should be used to close channels and streams
	 * and release resources. After this method returns without an error, the input is assumed to be correctly read.
	 * <p>
	 * When this method is called, the input format it guaranteed to be opened.
	 * 
	 * @throws IOException Thrown, if the input could not be closed properly.
	 */
	void close() throws IOException;
}
