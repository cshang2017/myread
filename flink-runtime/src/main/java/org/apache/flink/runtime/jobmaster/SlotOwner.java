package org.apache.flink.runtime.jobmaster;

/**
 * Interface for components that hold slots and to which slots get released / recycled.
 */
public interface SlotOwner {

	/**
	 * Return the given slot to the slot owner.
	 *
	 * @param logicalSlot to return
	 */
	void returnLogicalSlot(LogicalSlot logicalSlot);
}
