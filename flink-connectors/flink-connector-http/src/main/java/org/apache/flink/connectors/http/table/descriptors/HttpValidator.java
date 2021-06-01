package org.apache.flink.connectors.http.table.descriptors;

import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

public class HttpValidator extends ConnectorDescriptorValidator {

	public static final String CONNECTOR_TYPE_VALUE_HTTP= "http";
	public static final String CONNECTOR_ADDRESS = "connector.address";
	public final static String CONNECTOR_REQUEST_METHOD = "connector.request-method";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateValue(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_HTTP, false);

		properties.validateString(CONNECTOR_ADDRESS, false, 1, Integer.MAX_VALUE);
		properties.validateString(CONNECTOR_REQUEST_METHOD, false, 1, Integer.MAX_VALUE);
	}

}
