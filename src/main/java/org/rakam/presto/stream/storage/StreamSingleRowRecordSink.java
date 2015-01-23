package org.rakam.presto.stream.storage;

import com.facebook.presto.operator.aggregation.Accumulator;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/01/15 13:47.
 */
public class StreamSingleRowRecordSink implements ConnectorPageSink
{

    private final SingleRowTable table;

    public StreamSingleRowRecordSink(SingleRowTable table) {
        this.table = table;
    }

    @Override
    public void appendPage(Page page, Block sampleWeightBlock) {
        for (Accumulator accumulator : table.getAggregations()) {
            accumulator.addInput(page);
        }
    }

    @Override
    public String commit() {
        return "";
    }

}
