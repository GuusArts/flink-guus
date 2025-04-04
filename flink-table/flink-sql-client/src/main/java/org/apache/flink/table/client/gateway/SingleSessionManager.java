/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.client.gateway;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.resource.ClientResourceManager;
import org.apache.flink.table.client.util.ClientClassloaderUtil;
import org.apache.flink.table.client.util.ClientWrapperClassLoader;
import org.apache.flink.table.gateway.api.endpoint.EndpointVersion;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.table.gateway.service.context.SessionContext;
import org.apache.flink.table.gateway.service.operation.OperationExecutor;
import org.apache.flink.table.gateway.service.operation.OperationManager;
import org.apache.flink.table.gateway.service.result.ResultFetcher;
import org.apache.flink.table.gateway.service.session.Session;
import org.apache.flink.table.gateway.service.session.SessionManager;
import org.apache.flink.table.resource.ResourceType;
import org.apache.flink.table.resource.ResourceUri;
import org.apache.flink.util.MutableURLClassLoader;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.flink.client.cli.ArtifactFetchOptions.ARTIFACT_LIST;
import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.table.client.cli.CliUtils.isApplicationMode;

/**
 * A {@link SessionManager} only has one session at most. It uses the less resources and also
 * provides special handler for the REMOVE JAR syntax.
 *
 * <p>The special {@link SessionManager} is used in the Sql Client embedded mode and doesn't support
 * concurrently modification.
 */
public class SingleSessionManager implements SessionManager {

    private static final ConfigOption<List<String>> YARN_SHIP_FILES =
            key("yarn.ship-files").stringType().asList().noDefaultValue();

    private final DefaultContext defaultContext;
    private final ExecutorService operationExecutorService;

    private Session session;

    public SingleSessionManager(DefaultContext defaultContext) {
        this.defaultContext = defaultContext;
        this.operationExecutorService = Executors.newSingleThreadExecutor();
    }

    @Override
    public void start() {}

    @Override
    public void stop() {
        operationExecutorService.shutdown();
    }

    @Override
    public Session getSession(SessionHandle sessionHandle) throws SqlGatewayException {
        Preconditions.checkArgument(
                session != null && sessionHandle.equals(session.getSessionHandle()),
                "The specified session doesn't exists");
        return session;
    }

    @Override
    public Session openSession(SessionEnvironment environment) throws SqlGatewayException {
        if (session != null) {
            throw new SqlClientException(
                    String.format(
                            "The %s can only maintain one session at the same time. Please close the current session "
                                    + "before opening a new session.",
                            SingleSessionManager.class.getName()));
        }
        SessionHandle sessionHandle = SessionHandle.create();
        session =
                new Session(
                        EmbeddedSessionContext.create(
                                defaultContext,
                                sessionHandle,
                                environment,
                                operationExecutorService));

        session.open();
        return session;
    }

    @Override
    public void closeSession(SessionHandle sessionHandle) throws SqlGatewayException {
        Preconditions.checkArgument(
                session != null && sessionHandle.equals(session.getSessionHandle()),
                "The specified session doesn't exist.");
        session.close();
        session = null;
    }

    private static class EmbeddedSessionContext extends SessionContext {

        private EmbeddedSessionContext(
                DefaultContext defaultContext,
                SessionHandle sessionId,
                EndpointVersion endpointVersion,
                Configuration sessionConf,
                URLClassLoader classLoader,
                SessionState sessionState,
                OperationManager operationManager) {
            super(
                    defaultContext,
                    sessionId,
                    endpointVersion,
                    sessionConf,
                    classLoader,
                    sessionState,
                    operationManager);
        }

        public static EmbeddedSessionContext create(
                DefaultContext defaultContext,
                SessionHandle sessionId,
                SessionEnvironment environment,
                ExecutorService operationExecutorService) {
            Configuration configuration =
                    initializeConfiguration(defaultContext, environment, sessionId);
            List<URI> dependencies;
            // rewrite the dependencies
            if (isApplicationMode(configuration)) {
                dependencies = Collections.emptyList();
                if (!defaultContext.getDependencies().isEmpty()) {
                    String target = configuration.getOptional(DeploymentOptions.TARGET).orElse("");
                    if (target.equals("yarn-application")) {
                        configuration.set(
                                YARN_SHIP_FILES,
                                defaultContext.getDependencies().stream()
                                        .map(URI::toString)
                                        .collect(Collectors.toList()));
                    } else if (target.equals("kubernetes-application")) {
                        configuration.set(
                                ARTIFACT_LIST,
                                defaultContext.getDependencies().stream()
                                        .map(URI::toString)
                                        .collect(Collectors.toList()));
                    } else {
                        throw new SqlGatewayException("Unknown deployment target: " + target);
                    }
                }
            } else {
                dependencies = defaultContext.getDependencies();
            }
            final MutableURLClassLoader userClassLoader =
                    new ClientWrapperClassLoader(
                            ClientClassloaderUtil.buildUserClassLoader(
                                    Collections.emptyList(),
                                    SessionContext.class.getClassLoader(),
                                    new Configuration(configuration)),
                            configuration);
            ClientResourceManager resourceManager =
                    new ClientResourceManager(configuration, userClassLoader);
            try {
                resourceManager.registerJarResources(
                        dependencies.stream()
                                .map(uri -> new ResourceUri(ResourceType.JAR, uri.toString()))
                                .collect(Collectors.toList()));
                return new EmbeddedSessionContext(
                        defaultContext,
                        sessionId,
                        environment.getSessionEndpointVersion(),
                        configuration,
                        userClassLoader,
                        initializeSessionState(environment, configuration, resourceManager),
                        new OperationManager(operationExecutorService));
            } catch (IOException e) {
                throw new SqlGatewayException("Failed to open the session.", e);
            }
        }

        @Override
        public OperationExecutor createOperationExecutor(Configuration executionConfig) {
            return new EmbeddedOperationExecutor(this, executionConfig);
        }
    }

    private static class EmbeddedOperationExecutor extends OperationExecutor {

        public EmbeddedOperationExecutor(SessionContext context, Configuration executionConfig) {
            super(context, executionConfig);
        }

        @Override
        protected ResultFetcher callRemoveJar(OperationHandle operationHandle, String jarPath) {
            URL jarURL =
                    ((ClientResourceManager) sessionContext.getSessionState().resourceManager)
                            .unregisterJarResource(jarPath);
            if (jarURL != null) {
                ((ClientWrapperClassLoader)
                                sessionContext
                                        .getSessionState()
                                        .resourceManager
                                        .getUserClassLoader())
                        .removeURL(jarURL);
            }
            return ResultFetcher.fromTableResult(
                    operationHandle, TableResultInternal.TABLE_RESULT_OK, false);
        }
    }
}
