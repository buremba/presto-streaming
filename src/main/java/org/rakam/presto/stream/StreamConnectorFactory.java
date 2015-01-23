/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.rakam.presto.stream;

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import org.rakam.presto.stream.util.CurrentNodeId;
import org.rakam.presto.stream.util.RebindSafeMBeanServer;
import org.weakref.jmx.guice.MBeanModule;

import javax.management.MBeanServer;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.management.ManagementFactory.getPlatformMBeanServer;

public class StreamConnectorFactory
        implements ConnectorFactory
{
    private final TypeManager typeManager;
    private final Map<String, String> optionalConfig;
    private final MetadataManager metadataManager;
    private final NodeManager nodeManager;
    private final SqlParser sqlParser;
    private final FeaturesConfig featuresConfig;
    private final List<PlanOptimizer> planOptimizers;
    private final String name;
    private final Module module;

    public StreamConnectorFactory(String name, Module module, TypeManager typeManager, MetadataManager metadataManager, NodeManager nodeManager, List<PlanOptimizer> planOptimizers, FeaturesConfig featuresConfig, SqlParser sqlParser, Map<String, String> optionalConfig) {
        this.name = checkNotNull(name, "name is null");
        this.module = checkNotNull(module, "module is null");
        this.typeManager = checkNotNull(typeManager, "typeManager is null");
        this.metadataManager = checkNotNull(metadataManager, "metadataManager is null");
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
        this.optionalConfig = ImmutableMap.copyOf(checkNotNull(optionalConfig, "optionalConfig is null"));
        this.planOptimizers = checkNotNull(planOptimizers, "planOptimizers is null");
        this.featuresConfig = checkNotNull(featuresConfig, "featuresConfig is null");
        this.sqlParser = checkNotNull(sqlParser, "sqlParser is null");
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public Connector create(final String connectorId, Map<String, String> requiredConfig)
    {
        checkNotNull(requiredConfig, "requiredConfig is null");
        checkNotNull(optionalConfig, "optionalConfig is null");

        try {
            Bootstrap app = new Bootstrap(new StreamModule(connectorId),
                    new MBeanModule(), module,
                    binder -> {

                    CurrentNodeId currentNodeId = new CurrentNodeId(nodeManager.getCurrentNode().getNodeIdentifier());
                    MBeanServer mbeanServer = new RebindSafeMBeanServer(getPlatformMBeanServer());

                    binder.bind(MBeanServer.class).toInstance(mbeanServer);
                    binder.bind(CurrentNodeId.class).toInstance(currentNodeId);
                    binder.bind(NodeManager.class).toInstance(nodeManager);
                    binder.bind(TypeManager.class).toInstance(typeManager);
                    binder.bind(MetadataManager.class).toInstance(metadataManager);
                    binder.bind(SqlParser.class).toInstance(sqlParser);
                    binder.bind(FeaturesConfig.class).toInstance(featuresConfig);
                    binder.bind(new TypeLiteral<List<PlanOptimizer>>() {}).toInstance(planOptimizers);
            });

        Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();

            return injector.getInstance(StreamConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
