package org.rakam.presto.stream.analyze;

import com.facebook.presto.sql.tree.SortItem;
import org.rakam.presto.stream.analyze.QueryAnalyzer.AggregationField;
import org.rakam.presto.stream.analyze.QueryAnalyzer.GroupByField;

import java.util.List;
import java.util.Optional;

/**
 * Created by buremba <Burak Emre Kabakcı> on 22/01/15 15:42.
 */
public class AggregationQuery {
    public final Optional<String> limit;
    public final List<SortItem> orderBy;
    public final List<AggregationField> aggregationFields;
    public final List<GroupByField> groupByFields;

    public AggregationQuery(List<AggregationField> aggregationFields, List<GroupByField> groupByFields, List<SortItem> orderBy, Optional<String> limit) {
        this.limit = limit;
        this.orderBy = orderBy;
        this.aggregationFields = aggregationFields;
        this.groupByFields = groupByFields;
    }

    // since group by fields are required, if this is a group by query, there is at least one group by column.
    public boolean isGroupByQuery() {
        return !groupByFields.isEmpty();
    }
}
