package org.rakam.presto.stream.storage;

import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

/**
 * Created by buremba <Burak Emre Kabakcı> on 22/01/15 12:07.
 */
public interface GroupByStreamRow {
    public double getDouble(int groupId);
    public Slice getSlice(int groupId);
    public long getLong(int groupId);
    public boolean isNull(int groupId);
    public Type getType();
}
