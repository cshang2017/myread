package org.apache.flink.cep.nfa;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.EnumSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.cep.nfa.sharedbuffer.EventId;
import org.apache.flink.cep.nfa.sharedbuffer.NodeId;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Methods for deserialization of old format NFA.
 */
class MigrationUtils {

	/**
	 * Skips bytes corresponding to serialized states. In flink 1.6+ the states are no longer kept in state.
	 */
	static void skipSerializedStates(DataInputView in) throws IOException {
		TypeSerializer<String> nameSerializer = StringSerializer.INSTANCE;
		TypeSerializer<State.StateType> stateTypeSerializer = new EnumSerializer<>(State.StateType.class);
		TypeSerializer<StateTransitionAction> actionSerializer = new EnumSerializer<>(StateTransitionAction.class);

		final int noOfStates = in.readInt();

		for (int i = 0; i < noOfStates; i++) {
			nameSerializer.deserialize(in);
			stateTypeSerializer.deserialize(in);
		}

		for (int i = 0; i < noOfStates; i++) {
			String srcName = nameSerializer.deserialize(in);

			int noOfTransitions = in.readInt();
			for (int j = 0; j < noOfTransitions; j++) {
				String src = nameSerializer.deserialize(in);
				Preconditions.checkState(src.equals(srcName),
					"Source Edge names do not match (" + srcName + " - " + src + ").");

				nameSerializer.deserialize(in);
				actionSerializer.deserialize(in);

				try {
					skipCondition(in);
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private static void skipCondition(DataInputView in) throws IOException, ClassNotFoundException {
		boolean hasCondition = in.readBoolean();
		if (hasCondition) {
			int length = in.readInt();

			byte[] serCondition = new byte[length];
			in.read(serCondition);

			ByteArrayInputStream bais = new ByteArrayInputStream(serCondition);
			ObjectInputStream ois = new ObjectInputStream(bais);

			ois.readObject();
			ois.close();
			bais.close();
		}
	}

	static <T> Queue<ComputationState> deserializeComputationStates(
			org.apache.flink.cep.nfa.SharedBuffer<T> sharedBuffer,
			TypeSerializer<T> eventSerializer,
			DataInputView source) throws IOException {

		Queue<ComputationState> computationStates = new LinkedList<>();
		StringSerializer stateNameSerializer = StringSerializer.INSTANCE;
		LongSerializer timestampSerializer = LongSerializer.INSTANCE;
		DeweyNumber.DeweyNumberSerializer versionSerializer = DeweyNumber.DeweyNumberSerializer.INSTANCE;

		int computationStateNo = source.readInt();
		for (int i = 0; i < computationStateNo; i++) {
			String state = stateNameSerializer.deserialize(source);
			String prevState = stateNameSerializer.deserialize(source);
			long timestamp = timestampSerializer.deserialize(source);
			DeweyNumber version = versionSerializer.deserialize(source);
			long startTimestamp = timestampSerializer.deserialize(source);
			int counter = source.readInt();

			T event = null;
			if (source.readBoolean()) {
				event = eventSerializer.deserialize(source);
			}

			NodeId nodeId;
			EventId startEventId;
			if (prevState != null) {
				nodeId = sharedBuffer.getNodeId(prevState, timestamp, counter, event);
				startEventId = sharedBuffer.getStartEventId(version.getRun());
			} else {
				nodeId = null;
				startEventId = null;
			}

			computationStates.add(ComputationState.createState(state, nodeId, version, startTimestamp, startEventId));
		}
		return computationStates;
	}

	private MigrationUtils() {
	}
}
