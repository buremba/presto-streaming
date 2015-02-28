package org.rakam.presto.stream.storage;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorPageSinkProvider;
import com.google.inject.Inject;

import static com.facebook.presto.util.Types.checkType;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/01/15 13:45.
 */
public class StreamPageSinkProvider implements ConnectorPageSinkProvider {

    private final StreamStorageManager storageManager;

    @Inject
    public StreamPageSinkProvider(StreamStorageManager storageManager) {
        this.storageManager = storageManager;
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorOutputTableHandle tableHandle) {
        StreamInsertTableHandle handle = checkType(tableHandle, StreamInsertTableHandle.class, "tableHandle");

        MaterializedView materializedView = storageManager.get(handle.getTableId());
        if(materializedView instanceof SimpleRowTable) {
            return new SimpleStreamRecordSink((SimpleRowTable) materializedView);
        }else {
//            StreamMultipleRowRecordSink recordSink = new StreamMultipleRowRecordSink((MultipleRowTable) materializedView);
//            return new RecordPageSink(recordSink);
            return new GroupByStreamRecordSink((GroupByRowTable) materializedView);
        }
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorInsertTableHandle insertTableHandle) {
        StreamInsertTableHandle handle = checkType(insertTableHandle, StreamInsertTableHandle.class, "tableHandle");

        MaterializedView materializedView = storageManager.get(handle.getTableId());
        if(materializedView instanceof SimpleRowTable) {
            return new SimpleStreamRecordSink((SimpleRowTable) materializedView);
        }else {
//            StreamMultipleRowRecordSink recordSink = new StreamMultipleRowRecordSink((MultipleRowTable) materializedView);
//            return new RecordPageSink(recordSink);
            return new GroupByStreamRecordSink((GroupByRowTable) materializedView);
        }
    }
}
