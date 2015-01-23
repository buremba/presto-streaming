package org.rakam.presto.stream.storage;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.rakam.presto.stream.StreamColumnHandle;

import javax.annotation.Nullable;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by buremba <Burak Emre Kabakcı> on 19/01/15 12:10.
 */
public class StreamInsertTableHandle implements ConnectorInsertTableHandle {
    private final String connectorId;
    private final long tableId;
    private final List<StreamColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    @Nullable
    private final String externalBatchId;
    private final List<StreamColumnHandle> sortColumnHandles;
    private final List<SortOrder> sortOrders;

    @JsonCreator
    public StreamInsertTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("tableId") long tableId,
            @JsonProperty("columnHandles") List<StreamColumnHandle> columnHandles,
            @JsonProperty("columnTypes") List<Type> columnTypes,
            @JsonProperty("externalBatchId") @Nullable String externalBatchId,
            @JsonProperty("sortColumnHandles") List<StreamColumnHandle> sortColumnHandles,
            @JsonProperty("sortOrders") List<SortOrder> sortOrders) {
        checkArgument(tableId > 0, "tableId must be greater than zero");

        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.tableId = tableId;
        this.columnHandles = ImmutableList.copyOf(checkNotNull(columnHandles, "columnHandles is null"));
        this.columnTypes = ImmutableList.copyOf(checkNotNull(columnTypes, "columnTypes is null"));
        this.externalBatchId = externalBatchId;

        this.sortOrders = ImmutableList.copyOf(checkNotNull(sortOrders, "sortOrders is null"));
        this.sortColumnHandles = ImmutableList.copyOf(checkNotNull(sortColumnHandles, "sortColumnHandles is null"));
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }

    @JsonProperty
    public long getTableId() {
        return tableId;
    }

    @JsonProperty
    public List<StreamColumnHandle> getColumnHandles() {
        return columnHandles;
    }

    @JsonProperty
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @Nullable
    @JsonProperty
    public String getExternalBatchId() {
        return externalBatchId;
    }

    @JsonProperty
    public List<StreamColumnHandle> getSortColumnHandles() {
        return sortColumnHandles;
    }

    @JsonProperty
    public List<SortOrder> getSortOrders() {
        return sortOrders;
    }

    @Override
    public String toString() {
        return connectorId + ":" + tableId;
    }
}