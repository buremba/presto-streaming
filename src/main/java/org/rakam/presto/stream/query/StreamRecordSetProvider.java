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
package org.rakam.presto.stream.query;

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;
import org.rakam.presto.stream.StreamColumnHandle;
import org.rakam.presto.stream.StreamConnectorId;
import org.rakam.presto.stream.StreamSplit;
import org.rakam.presto.stream.metadata.StreamMetadata;
import org.rakam.presto.stream.metadata.Table;
import org.rakam.presto.stream.storage.MaterializedView;
import org.rakam.presto.stream.storage.StreamStorageManager;

import javax.inject.Inject;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.rakam.presto.stream.util.Types.checkType;

public class StreamRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final String connectorId;
    private final StreamMetadata metadata;
    private final StreamStorageManager storage;

    @Inject
    public StreamRecordSetProvider(StreamConnectorId connectorId, StreamMetadata metadata, StreamStorageManager storage)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.storage = checkNotNull(storage, "storage is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorSplit split, List<? extends ConnectorColumnHandle> columns)
    {
        checkNotNull(split, "partitionChunk is null");
        StreamSplit streamSplit = checkType(split, StreamSplit.class, "split");
        checkArgument(streamSplit.getConnectorId().equals(connectorId), "split is not for this connector");

        ImmutableList.Builder<StreamColumnHandle> handles = ImmutableList.builder();
        for (ConnectorColumnHandle handle : columns) {
            handles.add(checkType(handle, StreamColumnHandle.class, "handle"));
        }

        Table table = metadata.getTable(streamSplit.getSchemaName(), streamSplit.getTableName());

        MaterializedView materializedView = storage.get(table.getTableId());

        return new StreamRecordSet(streamSplit, handles.build(), materializedView);
    }
}
