package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.Internal;

import java.util.ArrayList;
import java.util.List;

/**
 * Validator for {@code StreamTableDescriptor}.
 */
@Internal
public class StreamTableDescriptorValidator implements DescriptorValidator {

	public static final String UPDATE_MODE = "update-mode";
	public static final String UPDATE_MODE_VALUE_APPEND = "append";
	public static final String UPDATE_MODE_VALUE_RETRACT = "retract";
	public static final String UPDATE_MODE_VALUE_UPSERT = "upsert";

	private final boolean supportsAppend;
	private final boolean supportsRetract;
	private final boolean supportsUpsert;

	public StreamTableDescriptorValidator(boolean supportsAppend, boolean supportsRetract, boolean supportsUpsert) {
		this.supportsAppend = supportsAppend;
		this.supportsRetract = supportsRetract;
		this.supportsUpsert = supportsUpsert;
	}

	@Override
	public void validate(DescriptorProperties properties) {
		List<String> modeList = new ArrayList<>();
		if (supportsAppend) {
			modeList.add(UPDATE_MODE_VALUE_APPEND);
		}
		if (supportsRetract) {
			modeList.add(UPDATE_MODE_VALUE_RETRACT);
		}
		if (supportsUpsert) {
			modeList.add(UPDATE_MODE_VALUE_UPSERT);
		}
		properties.validateEnumValues(
			UPDATE_MODE,
			false,
			modeList
		);
	}
}
