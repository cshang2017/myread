package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.operators.base.OuterJoinOperatorBase.OuterJoinType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.hash.NonReusingBuildFirstHashJoinIterator;
import org.apache.flink.runtime.operators.hash.NonReusingBuildSecondHashJoinIterator;
import org.apache.flink.runtime.operators.hash.ReusingBuildFirstHashJoinIterator;
import org.apache.flink.runtime.operators.hash.ReusingBuildSecondHashJoinIterator;
import org.apache.flink.runtime.operators.sort.NonReusingMergeOuterJoinIterator;
import org.apache.flink.runtime.operators.sort.ReusingMergeOuterJoinIterator;
import org.apache.flink.runtime.operators.util.JoinTaskIterator;
import org.apache.flink.util.MutableObjectIterator;

/**
 * The full outer join driver implements the logic of an outer join operator at runtime. It instantiates a sort-merge based strategy to find
 * joining pairs of records or joins records with null if no match is found.
 */
public class FullOuterJoinDriver<IT1, IT2, OT> extends AbstractOuterJoinDriver<IT1, IT2, OT> {
	
	@Override
	protected JoinTaskIterator<IT1, IT2, OT> getReusingOuterJoinIterator(
			DriverStrategy driverStrategy,
			MutableObjectIterator<IT1> in1,
			MutableObjectIterator<IT2> in2,
			TypeSerializer<IT1> serializer1,
			TypeComparator<IT1> comparator1,
			TypeSerializer<IT2> serializer2,
			TypeComparator<IT2> comparator2,
			TypePairComparatorFactory<IT1, IT2> pairComparatorFactory,
			MemoryManager memoryManager,
			IOManager ioManager,
			double driverMemFraction
	) throws Exception {
		switch (driverStrategy) {
			case FULL_OUTER_MERGE:
				int numPages = memoryManager.computeNumberOfPages(driverMemFraction);
				return new ReusingMergeOuterJoinIterator<>(
						OuterJoinType.FULL,
						in1,
						in2,
						serializer1,
						comparator1,
						serializer2,
						comparator2,
						pairComparatorFactory.createComparator12(comparator1, comparator2),
						memoryManager,
						ioManager,
						numPages,
						super.taskContext.getContainingTask()
				);
		case FULL_OUTER_HYBRIDHASH_BUILD_FIRST:
			return new ReusingBuildFirstHashJoinIterator<>(in1, in2,
					serializer1, comparator1,
					serializer2, comparator2,
					pairComparatorFactory.createComparator21(comparator1, comparator2),
					memoryManager, ioManager,
					this.taskContext.getContainingTask(),
					driverMemFraction,
					true,
					true,
					false);
		case FULL_OUTER_HYBRIDHASH_BUILD_SECOND:
			return new ReusingBuildSecondHashJoinIterator<>(in1, in2,
					serializer1, comparator1,
					serializer2, comparator2,
					pairComparatorFactory.createComparator12(comparator1, comparator2),
					memoryManager, ioManager,
					this.taskContext.getContainingTask(),
					driverMemFraction,
					true,
					true,
					false);
			default:
				throw new Exception("Unsupported driver strategy for full outer join driver: " + driverStrategy.name());
		}
	}
	
	@Override
	protected JoinTaskIterator<IT1, IT2, OT> getNonReusingOuterJoinIterator(
			DriverStrategy driverStrategy,
			MutableObjectIterator<IT1> in1,
			MutableObjectIterator<IT2> in2,
			TypeSerializer<IT1> serializer1,
			TypeComparator<IT1> comparator1,
			TypeSerializer<IT2> serializer2,
			TypeComparator<IT2> comparator2,
			TypePairComparatorFactory<IT1, IT2> pairComparatorFactory,
			MemoryManager memoryManager,
			IOManager ioManager,
			double driverMemFraction
	) throws Exception {
		switch (driverStrategy) {
			case FULL_OUTER_MERGE:
				int numPages = memoryManager.computeNumberOfPages(driverMemFraction);
				return new NonReusingMergeOuterJoinIterator<>(
						OuterJoinType.FULL,
						in1,
						in2,
						serializer1,
						comparator1,
						serializer2,
						comparator2,
						pairComparatorFactory.createComparator12(comparator1, comparator2),
						memoryManager,
						ioManager,
						numPages,
						super.taskContext.getContainingTask()
				);
			case FULL_OUTER_HYBRIDHASH_BUILD_FIRST:
				return new NonReusingBuildFirstHashJoinIterator<>(in1, in2,
					serializer1, comparator1,
					serializer2, comparator2,
					pairComparatorFactory.createComparator21(comparator1, comparator2),
					memoryManager, ioManager,
					this.taskContext.getContainingTask(),
					driverMemFraction,
					true,
					true,
					false);
			case FULL_OUTER_HYBRIDHASH_BUILD_SECOND:
				return new NonReusingBuildSecondHashJoinIterator<>(in1, in2,
					serializer1, comparator1,
					serializer2, comparator2,
					pairComparatorFactory.createComparator12(comparator1, comparator2),
					memoryManager, ioManager,
					this.taskContext.getContainingTask(),
					driverMemFraction,
					true,
					true,
					false);
			default:
				throw new Exception("Unsupported driver strategy for full outer join driver: " + driverStrategy.name());
		}
	}
}
