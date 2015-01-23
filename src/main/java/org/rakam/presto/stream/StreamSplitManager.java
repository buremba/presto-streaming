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

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import javax.inject.Inject;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.rakam.presto.stream.util.Types.checkType;

public class StreamSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final NodeManager nodeManager;
    private static final HashFunction hashFunction = Hashing.murmur3_128();

    @Inject
    public StreamSplitManager(StreamConnectorId connectorId, NodeManager nodeManager)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
    }

    @Override
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle tableHandle, TupleDomain<ConnectorColumnHandle> tupleDomain)
    {
        StreamTableHandle streamTableHandle = checkType(tableHandle, StreamTableHandle.class, "tableHandle");

        List<ConnectorPartition> partitions = ImmutableList.<ConnectorPartition>of(new StreamPartition(streamTableHandle.getSchemaName(), streamTableHandle.getTableName()));
        return new ConnectorPartitionResult(partitions, tupleDomain);
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle tableHandle, List<ConnectorPartition> partitions)
    {
        checkNotNull(partitions, "partitions is null");
        checkArgument(partitions.size() == 1, "Expected one partition but got %s", partitions.size());
        ConnectorPartition partition = partitions.get(0);

        StreamPartition examplePartition = checkType(partition, StreamPartition.class, "partition");

        String identifier = examplePartition.getSchemaName() + ":" + examplePartition.getTableName();
        Node[] nodes = nodeManager.getActiveNodes().stream()
                .sorted((x,y) -> x.getNodeIdentifier().compareTo(y.getNodeIdentifier())).toArray(Node[]::new);

        int idx = hashFunction.hashUnencodedChars(identifier).asInt() % nodes.length;
        List<ConnectorSplit> splits = Lists.newArrayList();
        splits.add(new StreamSplit(connectorId, examplePartition.getSchemaName(), examplePartition.getTableName(), nodes[idx].getHostAndPort()));

        return new FixedSplitSource(connectorId, splits);
    }
}
