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
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;

public class StreamPlugin
        implements Plugin
{
    private final String name;
    private final Module module;
    private TypeManager typeManager;
    private Map<String, String> optionalConfig = ImmutableMap.of();
    private NodeManager nodeManager;
    private MetadataManager metadataManager;
    private List<PlanOptimizer> planOptimizers;
    private FeaturesConfig featuresConfig;
    private SqlParser sqlParser;

    public StreamPlugin()
    {
        this(getPluginInfo());
    }

    public StreamPlugin(PluginInfo info) {
        this(info.getName(), info.getModule());
    }

    public StreamPlugin(String name, Module module)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.module = checkNotNull(module, "module is null");
    }

    @Override
    public synchronized void setOptionalConfig(Map<String, String> optionalConfig)
    {
        this.optionalConfig = ImmutableMap.copyOf(checkNotNull(optionalConfig, "optionalConfig is null"));
    }

    @Inject
    public synchronized void setTypeManager(TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    public synchronized Map<String, String> getOptionalConfig()
    {
        return optionalConfig;
    }

    @Override
    public synchronized <T> List<T> getServices(Class<T> type)
    {
        if (type == ConnectorFactory.class) {
            return ImmutableList.of(type.cast(
                    new StreamConnectorFactory(
                            name, module,
                            typeManager,
                            metadataManager,
                            nodeManager,
                            planOptimizers,
                            featuresConfig,
                            sqlParser,
                            getOptionalConfig())));
        }
        return ImmutableList.of();
    }

    @Inject
    public void setNodeManager(NodeManager nodeManager)
    {
        this.nodeManager = nodeManager;
    }

    @Inject
    public void setMetadataManager(MetadataManager metadataManager)
    {
        this.metadataManager = metadataManager;
    }

    @Inject
    public void setPlanOptimizers(List<PlanOptimizer> metadataManager)
    {
        this.planOptimizers = metadataManager;
    }

    @Inject
    public void setFeaturesConfig(FeaturesConfig featuresConfig)
    {
        this.featuresConfig = featuresConfig;
    }

    @Inject
    public void setSqlParser(SqlParser sqlParser)
    {
        this.sqlParser = sqlParser;
    }

    private static PluginInfo getPluginInfo()
    {
        ClassLoader classLoader = StreamPlugin.class.getClassLoader();
        ServiceLoader<PluginInfo> loader = ServiceLoader.load(PluginInfo.class, classLoader);
        List<PluginInfo> list = ImmutableList.copyOf(loader);
        return list.isEmpty() ? new PluginInfo() : getOnlyElement(list);
    }
}
