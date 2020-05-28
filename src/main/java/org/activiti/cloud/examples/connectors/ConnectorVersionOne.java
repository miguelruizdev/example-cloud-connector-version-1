/*
 * Copyright 2019 Alfresco, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.activiti.cloud.examples.connectors;

import java.util.HashMap;
import java.util.Map;

import org.activiti.api.process.model.IntegrationContext;
import org.activiti.cloud.api.process.model.IntegrationRequest;
import org.activiti.cloud.connectors.starter.channels.IntegrationResultSender;
import org.activiti.cloud.connectors.starter.configuration.ConnectorProperties;
import org.activiti.cloud.connectors.starter.model.IntegrationResultBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.stereotype.Component;

@Component
@EnableBinding(ConnectorVersionOneChannels.class)
public class ConnectorVersionOne {

    private Logger logger = LoggerFactory.getLogger(ConnectorVersionOne.class);

    private IntegrationResultSender integrationResultSender;
    private ConnectorProperties connectorProperties;

    public ConnectorVersionOne(IntegrationResultSender integrationResultSender,
                               ConnectorProperties connectorProperties) {
        this.integrationResultSender = integrationResultSender;
        this.connectorProperties = connectorProperties;
    }

    @StreamListener(value = ConnectorVersionOneChannels.CONNECTOR_ONE_CONSUMER, condition = "headers['appVersion']=='${application.version}'")
    public void receive(IntegrationRequest integrationRequest) {
        IntegrationContext integrationContext = integrationRequest.getIntegrationContext();
        Map<String, Object> inBoundVariables = integrationContext.getInBoundVariables();

        logger.info("hello from CONNECTOR VERSION ONE");

        logger.info(">>inbound: " + inBoundVariables);

        Map<String,Object> variableMap = Map.of("variable",
                                                "valueFromConnectorVersionOne");

        integrationResultSender.send(IntegrationResultBuilder
                                             .resultFor(integrationRequest, connectorProperties)
                                             .withOutboundVariables(variableMap)
                                             .buildMessage());

        logger.info(">>outbound: " + integrationContext.getOutBoundVariables().toString());

        logger.info("bye from CONNECTOR VERSION ONE");
    }

}
