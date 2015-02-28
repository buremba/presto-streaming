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

import com.facebook.presto.operator.aggregation.Accumulator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.FixedWidthBlockBuilder;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import org.rakam.presto.stream.StreamColumnHandle;
import org.rakam.presto.stream.storage.SimpleRowTable;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class SingleRowRecordCursor
        implements RecordCursor
{

    private final List<StreamColumnHandle> columnHandles;
    private final Accumulator[] table;
    boolean firstRow = true;

    public SingleRowRecordCursor(List<StreamColumnHandle> columnHandles, SimpleRowTable table)
    {
        this.columnHandles = columnHandles;
        this.table = table.getAggregations();
    }

    @Override
    public long getTotalBytes()
    {
        return Arrays.stream(table).mapToLong(a -> a.getEstimatedSize()).sum();
    }

    @Override
    public long getCompletedBytes()
    {
        return getTotalBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if(firstRow) {
            firstRow = false;
            return true;
        }
        return false;
    }

    @Override
    public boolean getBoolean(int field)
    {
        throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "Accumulators does not support booleans.");
    }

    @Override
    public long getLong(int field)
    {
        FixedWidthBlockBuilder blockBuilder = new FixedWidthBlockBuilder(8, 1);
        table[field].evaluateFinal(blockBuilder);
        Block build = blockBuilder.build();
        return build.getLong(0, 0);
    }

    @Override
    public double getDouble(int field)
    {
        FixedWidthBlockBuilder blockBuilder = new FixedWidthBlockBuilder(8, 1);
        table[field].evaluateFinal(blockBuilder);
        Block build = blockBuilder.build();
        return build.getDouble(0, 0);
    }

    @Override
    public Slice getSlice(int field)
    {
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus());
        table[field].evaluateFinal(blockBuilder);
        Block build = blockBuilder.build();
        return build.getSlice(0, 0, build.getLength(0));
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return false;
    }

    @Override
    public void close()
    {
    }
}
