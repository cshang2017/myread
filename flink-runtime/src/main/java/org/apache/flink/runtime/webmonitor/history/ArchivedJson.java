
package org.apache.flink.runtime.webmonitor.history;

import org.apache.flink.runtime.history.FsJobArchivist;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Objects;

/**
 * A simple container for a handler's JSON response and the REST URLs for which the response would've been returned.
 *
 * <p>These are created by {@link JsonArchivist}s, and used by the {@link FsJobArchivist} to create a directory structure
 * resembling the REST API.
 */
public class ArchivedJson {

	private static final ObjectMapper MAPPER = RestMapperUtils.getStrictObjectMapper();

	private final String path;
	private final String json;

	public ArchivedJson(String path, String json) {
		this.path = Preconditions.checkNotNull(path);
		this.json = Preconditions.checkNotNull(json);
	}

	public ArchivedJson(String path, ResponseBody json) throws IOException {
		this.path = Preconditions.checkNotNull(path);
		StringWriter sw = new StringWriter();
		MAPPER.writeValue(sw, Preconditions.checkNotNull(json));
		this.json = sw.toString();
	}

	public String getPath() {
		return path;
	}

	public String getJson() {
		return json;
	}


}
