package org.apache.flink.runtime.rest.handler;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;

import java.util.Collection;
import java.util.Collections;

/**
 * Rest handler interface which all rest handler implementation have to implement.
 */
public interface RestHandlerSpecification {

	/**
	 * Returns the {@link HttpMethodWrapper} to be used for the request.
	 *
	 * @return http method to be used for the request
	 */
	HttpMethodWrapper getHttpMethod();

	/**
	 * Returns the generalized endpoint url that this request should be sent to, for example {@code /job/:jobid}.
	 *
	 * @return endpoint url that this request should be sent to
	 */
	String getTargetRestEndpointURL();

	/**
	 * Returns the supported API versions that this request supports.
	 *
	 * @return Collection of supported API versions
	 */
	default Collection<RestAPIVersion> getSupportedAPIVersions() {
		return Collections.singleton(RestAPIVersion.V1);
	}
}
