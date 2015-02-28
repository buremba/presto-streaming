package org.rakam.presto.stream.query;

import com.facebook.presto.operator.GroupByHash;
import com.facebook.presto.operator.aggregation.GroupedAccumulator;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.FixedWidthBlockBuilder;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import org.rakam.presto.stream.StreamColumnHandle;
import org.rakam.presto.stream.storage.GroupByStreamRow;
import org.rakam.presto.stream.storage.GroupByRowTable;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/01/15 13:01.
 */
public class GroupByStreamRecordCursor implements RecordCursor {
    private final GroupByStreamRow[] fieldHandlers;
    private final GroupByHash groupByHash;
    private int groupId = -1;

    public GroupByStreamRecordCursor(List<StreamColumnHandle> columnHandles, GroupByRowTable table) {

        groupByHash = table.getGroupByHash();

        List<GroupByStreamRow> fields = new ArrayList<>();
        int aggIdx = 0;
        int groupByIdx = 0;
        PageBuilder pageBuilder = new PageBuilder(groupByHash.getTypes());
        for (StreamColumnHandle columnHandle : columnHandles) {
            if(columnHandle.getIsAggregationField()) {
                fields.add(new GroupByAccumulatorRow(table.getGroupedAggregations()[aggIdx++]));
            } else {
                fields.add(new GroupByKeyRow(groupByHash, pageBuilder, groupByIdx++));
            }
        }

        fieldHandlers = fields.stream().toArray(GroupByStreamRow[]::new);
    }

    @Override
    public long getTotalBytes() {
        return 0;
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Type getType(int field) {
        return fieldHandlers[field].getType();
    }

    @Override
    public boolean advanceNextPosition() {
        groupId++;
        return groupId < groupByHash.getGroupCount();
    }

    @Override
    public boolean getBoolean(int field) {
        throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "Accumulators does not support booleans.");
    }

    // TODO: Check if virtual method has performance drawback.
    @Override
    public long getLong(int field) {
        return fieldHandlers[field].getLong(groupId);
    }

    @Override
    public double getDouble(int field) {
        return fieldHandlers[field].getDouble(groupId);
    }

    @Override
    public Slice getSlice(int field) {
        return fieldHandlers[field].getSlice(groupId);
    }

    @Override
    public boolean isNull(int field) {
        return fieldHandlers[field].isNull(groupId);
    }

    @Override
    public void close() {
    }

    public class GroupByAccumulatorRow implements GroupByStreamRow {
        private final GroupedAccumulator accumulators;

        public GroupByAccumulatorRow(GroupedAccumulator accumulators) {
            this.accumulators = accumulators;
        }

        @Override
        public double getDouble(int groupId) {
            FixedWidthBlockBuilder blockBuilder = new FixedWidthBlockBuilder(8, 1);
            accumulators.evaluateFinal(groupId, blockBuilder);
            Block build = blockBuilder.build();
            return build.getDouble(0, 0);
        }

        @Override
        public Slice getSlice(int groupId) {
            FixedWidthBlockBuilder blockBuilder = new FixedWidthBlockBuilder(8, 1);
            accumulators.evaluateFinal(groupId, blockBuilder);
            Block build = blockBuilder.build();
            return build.getSlice(0, 0, build.getLength(0));
        }

        @Override
        public long getLong(int groupId) {
            FixedWidthBlockBuilder blockBuilder = new FixedWidthBlockBuilder(8, 1);
            accumulators.evaluateFinal(groupId, blockBuilder);
            Block build = blockBuilder.build();
            return build.getLong(0, 0);
        }

        @Override
        public boolean isNull(int groupId) {
            FixedWidthBlockBuilder blockBuilder = new FixedWidthBlockBuilder(8, 1);
            accumulators.evaluateFinal(groupId, blockBuilder);
            Block build = blockBuilder.build();
            return build.isNull(0);
        }

        @Override
        public Type getType() {
            return accumulators.getFinalType();
        }
    }

    public class GroupByKeyRow implements GroupByStreamRow {
        private final GroupByHash groupByHash;
        private final PageBuilder pageBuilder;
        private final int idx;

        public GroupByKeyRow(GroupByHash groupByHash, PageBuilder pageBuilder, int idx) {
            this.groupByHash = groupByHash;
            this.pageBuilder = pageBuilder;
            this.idx = idx;
        }

        private Block extractBlock(int groupId) {
            pageBuilder.reset();
            pageBuilder.declarePosition();
            groupByHash.appendValuesTo(groupId, pageBuilder, 0);
            return pageBuilder.build().getBlock(idx);
        }

        @Override
        public double getDouble(int groupId) {
            return extractBlock(groupId).getDouble(0, 0);
        }

        @Override
        public Slice getSlice(int groupId) {
            Block block = extractBlock(groupId);
            return block.getSlice(0, 0, block.getLength(0));
        }

        @Override
        public long getLong(int groupId) {
            return extractBlock(groupId).getLong(0, 0);
        }

        @Override
        public boolean isNull(int groupId) {
            return extractBlock(groupId).isNull(0);
        }

        @Override
        public Type getType() {
            return groupByHash.getTypes().get(idx);
        }
    }

}
