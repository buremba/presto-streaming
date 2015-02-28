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

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import org.rakam.presto.stream.StreamColumnHandle;
import org.rakam.presto.stream.StreamSplit;
import org.rakam.presto.stream.storage.MaterializedView;
import org.rakam.presto.stream.storage.GroupByRowTable;
import org.rakam.presto.stream.storage.SimpleRowTable;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class StreamRecordSet implements RecordSet
{
    private final List<StreamColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final MaterializedView view;

    public StreamRecordSet(StreamSplit split, List<StreamColumnHandle> columnHandles, MaterializedView view)
    {
        checkNotNull(split, "split is null");

        this.columnHandles = checkNotNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (StreamColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
        this.view = view;
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        if(view instanceof SimpleRowTable) {
            return new SingleRowRecordCursor(columnHandles, (SimpleRowTable) view);
        }else {
            return new GroupByStreamRecordCursor(columnHandles, (GroupByRowTable) view);
        }
    }
}
