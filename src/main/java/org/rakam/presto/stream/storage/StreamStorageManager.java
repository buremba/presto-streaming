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
package org.rakam.presto.stream.storage;

import com.facebook.presto.operator.GroupByHash;
import com.facebook.presto.operator.aggregation.Accumulator;
import com.facebook.presto.operator.aggregation.GroupedAccumulator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.Maps;
import io.airlift.log.Logger;
import org.rakam.presto.stream.analyze.QueryAnalyzer;
import org.rakam.presto.stream.metadata.ForMetadata;
import org.rakam.presto.stream.metadata.MetadataDao;
import org.rakam.presto.stream.metadata.StreamMetadata;
import org.rakam.presto.stream.metadata.TableColumn;
import org.skife.jdbi.v2.IDBI;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static java.lang.String.format;

public class StreamStorageManager
{
    private static final Logger log = Logger.get(StreamMetadata.class);
    private final MetadataDao dao;
    private final QueryAnalyzer queryAnalyzer;

    Map<Long, MaterializedView> tables;

    @Inject
    public StreamStorageManager(@ForMetadata IDBI dbi, QueryAnalyzer queryAnalyzer)
    {
        this.tables = Maps.newConcurrentMap();
        this.dao = dbi.onDemand(MetadataDao.class);
        this.queryAnalyzer = queryAnalyzer;
//        schemas = DBMaker.newFileDB(new File("./test")).make();
    }

    public void addMaterializedView(long tableId, MaterializedView view) {
        tables.put(tableId, view);
    }

    public MaterializedView get(long tableId) {
        MaterializedView materializedView = tables.get(tableId);
        if(materializedView==null) {
            log.warn("Table couldn't found. Creating new one from table metadata. ");
            MaterializedView value = reGenerateTable(tableId);
            tables.put(tableId, value);
            return value;
        }
        return materializedView;
    }

    private MaterializedView reGenerateTable(long tableId) {
        List<TableColumn> columns = getColumnsOfTable(tableId);

        boolean isGroupByQuery = columns.stream().anyMatch(x -> !x.getIsAggregationField());

        if (!isGroupByQuery) {
            return new SingleRowTable(columns.stream()
                    .map(x -> queryAnalyzer.getAccumulatorFactory(x.getSignature()).createAccumulator())
                    .toArray(Accumulator[]::new));
        } else {
            GroupedAccumulator[] groupedAggregations = columns.stream()
                    .filter(x -> x.getIsAggregationField())
                    .map(x -> queryAnalyzer.getAccumulatorFactory(x.getSignature()).createGroupedAccumulator())
                    .toArray(GroupedAccumulator[]::new);

            List<Type> types = columns.stream()
                    .filter(x -> !x.getIsAggregationField())
                    .map(x -> x.getDataType()).collect(Collectors.toList());
            int[] positions = columns.stream()
                    .filter(x -> !x.getIsAggregationField())
                    .mapToInt(x -> x.getOrdinalPosition()).toArray();

            GroupByHash groupByHash = new GroupByHash(types, positions, Optional.empty(), 10000);
            return new MultipleRowTable(groupedAggregations, groupByHash, positions);
        }
    }

    public List<TableColumn> getColumnsOfTable(long tableId)
    {
        List<TableColumn> tableColumn = dao.getTableColumns(tableId);
        if (tableColumn == null) {
            throw new PrestoException(NOT_FOUND, format("Table %s does not exist.", tableId));
        }
        return tableColumn;
    }
}
