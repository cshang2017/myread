package org.apache.flink.runtime.rest.handler;

import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Simple container for the request to a handler, that contains the {@link RequestBody} and path/query parameters.
 *
 * @param <R> type of the contained request body
 * @param <M> type of the contained message parameters
 */
public class HandlerRequest<R extends RequestBody, M extends MessageParameters> {

	private final R requestBody;
	private final Collection<File> uploadedFiles;
	private final Map<Class<? extends MessagePathParameter<?>>, MessagePathParameter<?>> pathParameters = new HashMap<>(2);
	private final Map<Class<? extends MessageQueryParameter<?>>, MessageQueryParameter<?>> queryParameters = new HashMap<>(2);

	public HandlerRequest(R requestBody, M messageParameters) throws HandlerRequestException {
		this(requestBody, messageParameters, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyList());
	}

	public HandlerRequest(R requestBody, M messageParameters, Map<String, String> receivedPathParameters, Map<String, List<String>> receivedQueryParameters) throws HandlerRequestException {
		this(requestBody, messageParameters, receivedPathParameters, receivedQueryParameters, Collections.emptyList());
	}

	public HandlerRequest(R requestBody, M messageParameters, Map<String, String> receivedPathParameters, Map<String, List<String>> receivedQueryParameters, Collection<File> uploadedFiles) throws HandlerRequestException {
		this.requestBody = Preconditions.checkNotNull(requestBody);
		this.uploadedFiles = Collections.unmodifiableCollection(Preconditions.checkNotNull(uploadedFiles));
		Preconditions.checkNotNull(messageParameters);
		Preconditions.checkNotNull(receivedQueryParameters);
		Preconditions.checkNotNull(receivedPathParameters);

		for (MessagePathParameter<?> pathParameter : messageParameters.getPathParameters()) {
			String value = receivedPathParameters.get(pathParameter.getKey());
			if (value != null) {
				pathParameter.resolveFromString(value);
							
				Class<? extends MessagePathParameter<?>> clazz = (Class<? extends MessagePathParameter<?>>) pathParameter.getClass();
				pathParameters.put(clazz, pathParameter);
			}
		}

		for (MessageQueryParameter<?> queryParameter : messageParameters.getQueryParameters()) {
			List<String> values = receivedQueryParameters.get(queryParameter.getKey());
			if (values != null && !values.isEmpty()) {
				StringJoiner joiner = new StringJoiner(",");
				values.forEach(joiner::add);

				queryParameter.resolveFromString(joiner.toString());
				
				Class<? extends MessageQueryParameter<?>> clazz = (Class<? extends MessageQueryParameter<?>>) queryParameter.getClass();
				queryParameters.put(clazz, queryParameter);
			}

		}
	}

	/**
	 * Returns the request body.
	 *
	 * @return request body
	 */
	public R getRequestBody() {
		return requestBody;
	}

	/**
	 * Returns the value of the {@link MessagePathParameter} for the given class.
	 *
	 * @param parameterClass class of the parameter
	 * @param <X>            the value type that the parameter contains
	 * @param <PP>           type of the path parameter
	 * @return path parameter value for the given class
	 * @throws IllegalStateException if no value is defined for the given parameter class
	 */
	public <X, PP extends MessagePathParameter<X>> X getPathParameter(Class<PP> parameterClass) {
		@SuppressWarnings("unchecked")
		PP pathParameter = (PP) pathParameters.get(parameterClass);
		Preconditions.checkState(pathParameter != null, "No parameter could be found for the given class.");
		return pathParameter.getValue();
	}

	/**
	 * Returns the value of the {@link MessageQueryParameter} for the given class.
	 *
	 * @param parameterClass class of the parameter
	 * @param <X>            the value type that the parameter contains
	 * @param <QP>           type of the query parameter
	 * @return query parameter value for the given class, or an empty list if no parameter value exists for the given class
	 */
	public <X, QP extends MessageQueryParameter<X>> List<X> getQueryParameter(Class<QP> parameterClass) {
		@SuppressWarnings("unchecked")
		QP queryParameter = (QP) queryParameters.get(parameterClass);
		if (queryParameter == null) {
			return Collections.emptyList();
		} else {
			return queryParameter.getValue();
		}
	}

	@Nonnull
	public Collection<File> getUploadedFiles() {
		return uploadedFiles;
	}
}
