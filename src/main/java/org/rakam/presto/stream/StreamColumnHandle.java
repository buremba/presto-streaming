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
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;

public final class StreamColumnHandle
        implements ConnectorColumnHandle
{
    private final String connectorId;
    private final String columnName;
    private final Type columnType;
    private final int columnOrdinal;
    private final boolean isAggregationField;

    @JsonCreator
    public StreamColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnOrdinal") int columnOrdinal,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("isAggregationField") boolean isAggregationField)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.columnName = checkNotNull(columnName, "columnName is null");
        this.columnType = checkNotNull(columnType, "columnType is null");
        this.columnOrdinal = columnOrdinal;
        this.isAggregationField = isAggregationField;
    }

    @JsonProperty
    public int getColumnOrdinal()
    {
        return columnOrdinal;
    }

    @JsonProperty
    public boolean getIsAggregationField()
    {
        return isAggregationField;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public Type getColumnType()
    {
        return columnType;
    }

    @Override
    public String toString()
    {
        return connectorId + ":" + columnName + ":" + columnType;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        StreamColumnHandle other = (StreamColumnHandle) obj;
        return java.util.Objects.equals(this.columnName, other.columnName);
    }

    @Override
    public int hashCode()
    {
        return java.util.Objects.hashCode(columnName);
    }

    public Type getType() {
        return columnType;
    }
}
