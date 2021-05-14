package org.apache.flink.runtime.jobmanager.scheduler;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Preconditions;

/**
 * A Co-location group is a group of JobVertices, where the <i>i-th</i> subtask of one vertex
 * has to be executed on the same TaskManager as the <i>i-th</i> subtask of all
 * other JobVertices in the same group.
 * 
 * <p>The co-location group is used for example to make sure that the i-th subtasks for iteration
 * head and iteration tail are scheduled to the same TaskManager.</p>
 */
public class CoLocationGroup implements java.io.Serializable {
	
	/** The ID that describes the slot co-location-constraint as a group */ 
	private final AbstractID id = new AbstractID();
	
	/** The vertices participating in the co-location group */
	private final List<JobVertex> vertices = new ArrayList<JobVertex>();
	
	/** The constraints, which hold the shared slots for the co-located operators */
	private transient ArrayList<CoLocationConstraint> constraints;
	
	// --------------------------------------------------------------------------------------------
	
	public CoLocationGroup() {}
	
	public CoLocationGroup(JobVertex... vertices) {
		for (JobVertex v : vertices) {
			this.vertices.add(v);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	public void addVertex(JobVertex vertex) {
		Preconditions.checkNotNull(vertex);
		this.vertices.add(vertex);
	}

	public List<JobVertex> getVertices() {
		return vertices;
	}
	
	public void mergeInto(CoLocationGroup other) {
		Preconditions.checkNotNull(other);
		
		for (JobVertex v : this.vertices) {
			v.updateCoLocationGroup(other);
		}
		
		// move vertex membership
		other.vertices.addAll(this.vertices);
		this.vertices.clear();
	}
	
	// --------------------------------------------------------------------------------------------
	
	public CoLocationConstraint getLocationConstraint(int subtask) {
		ensureConstraints(subtask + 1);
		return constraints.get(subtask);
	}
	
	private void ensureConstraints(int num) {
		if (constraints == null) {
			constraints = new ArrayList<CoLocationConstraint>(num);
		} else {
			constraints.ensureCapacity(num);
		}
		
		if (num > constraints.size()) {
			constraints.ensureCapacity(num);
			for (int i = constraints.size(); i < num; i++) {
				constraints.add(new CoLocationConstraint(this));
			}
		}
	}

	/**
	 * Gets the ID that identifies this co-location group.
	 * 
	 * @return The ID that identifies this co-location group.
	 */
	public AbstractID getId() {
		return id;
	}

	/**
	 * Resets this co-location group, meaning that future calls to {@link #getLocationConstraint(int)}
	 * will give out new CoLocationConstraints.
	 * 
	 * <p>This method can only be called when no tasks from any of the CoLocationConstraints are
	 * executed any more.</p>
	 */
	public void resetConstraints() {
		this.constraints.clear();
	}
}
