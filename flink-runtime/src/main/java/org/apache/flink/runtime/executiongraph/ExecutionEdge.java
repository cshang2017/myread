package org.apache.flink.runtime.executiongraph;

@Getter
@AllArgsConstructor
public class ExecutionEdge {

	private final IntermediateResultPartition source;

	private final ExecutionVertex target;

	private final int inputNum;

}
