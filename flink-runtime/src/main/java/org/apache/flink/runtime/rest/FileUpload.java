package org.apache.flink.runtime.rest;

import java.nio.file.Path;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public final class FileUpload {
	private final Path file;
	private final String contentType;
}
