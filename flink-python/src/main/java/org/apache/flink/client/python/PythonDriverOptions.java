package org.apache.flink.client.python;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Options for the {@link PythonDriver}.
 */
final class PythonDriverOptions {

	@Nullable
	private String entryPointModule;

	@Nullable
	private String entryPointScript;

	@Nonnull
	private List<String> programArgs;

	@Nullable
	String getEntryPointModule() {
		return entryPointModule;
	}

	Optional<String> getEntryPointScript() {
		return Optional.ofNullable(entryPointScript);
	}

	@Nonnull
	List<String> getProgramArgs() {
		return programArgs;
	}

	PythonDriverOptions(
		@Nullable String entryPointModule,
		@Nullable String entryPointScript,
		List<String> programArgs) {
		this.entryPointModule = entryPointModule;
		this.entryPointScript = entryPointScript;
		this.programArgs = requireNonNull(programArgs, "programArgs");
	}
}
