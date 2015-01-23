package org.rakam.presto.stream.query;

import com.facebook.presto.operator.GroupByHash;
import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.operator.aggregation.GroupedAccumulator;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import org.rakam.presto.stream.storage.MultipleRowTable;

/**
 * Created by buremba <Burak Emre Kabakcı> on 21/01/15 11:23.
 */
public class StreamMultipleRowRecordSink implements ConnectorPageSink {

    MultipleRowTable table;

    public StreamMultipleRowRecordSink(MultipleRowTable table) {
        this.table = table;
    }

    @Override
    public void appendPage(Page page, Block sampleWeightBlock) {
        GroupByHash groupByHash = table.getGroupByHash();
        GroupByIdBlock groupIds = groupByHash.getGroupIds(page);

        GroupedAccumulator[] groupedAggregations = table.getGroupedAggregations();

        int aggregationIdx = 0;
        int channelCount = page.getChannelCount();
        for (int i = 0; i < channelCount; i++) {
            if(table.isAggregationChannel(i)) {
                Block block = page.getBlock(i);
                groupedAggregations[aggregationIdx++].addIntermediate(groupIds, block);
            }


        }
    }

    @Override
    public String commit() {
        return "";
    }

}
