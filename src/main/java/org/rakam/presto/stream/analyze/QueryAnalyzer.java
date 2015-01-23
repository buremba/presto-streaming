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
package org.rakam.presto.stream.analyze;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragmenter;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Created by buremba <Burak Emre Kabakcı> on 16/01/15 22:44.
 */
public class QueryAnalyzer
{
    private final SqlParser sqlParser;
    private final boolean experimentalSyntaxEnabled;
    private final MetadataManager metadataManager;
    private List<PlanOptimizer> planOptimizers;

    @Inject
    public QueryAnalyzer(List<PlanOptimizer> planOptimizers, FeaturesConfig featuresConfig, SqlParser sqlParser, MetadataManager metadataManager)
    {
        this.sqlParser = sqlParser;
        this.planOptimizers = planOptimizers;
        this.metadataManager = metadataManager;
        this.experimentalSyntaxEnabled = featuresConfig.isExperimentalSyntaxEnabled();
    }

    public AccumulatorFactory getAccumulatorFactory(Signature signature) {
        FunctionInfo exactFunction = metadataManager.getExactFunction(signature);
        InternalAggregationFunction aggregationFunction = exactFunction.getAggregationFunction();
        return aggregationFunction
                .bind(ImmutableList.of(0), Optional.<Integer>empty(), Optional.<Integer>empty(), 1.0);
    }

    public AggregationQuery execute(Session session, String sql)
    {
        Statement statement = sqlParser.createStatement(sql);

        QuerySpecification queryBody = (QuerySpecification) ((Query) statement).getQueryBody();


        if(queryBody.getSelect().isDistinct())
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Distinct query is not supported");

        QueryExplainer explainer = new QueryExplainer(session, planOptimizers, metadataManager, sqlParser, experimentalSyntaxEnabled);
        Analyzer analyzer = new Analyzer(session, metadataManager, sqlParser, Optional.of(explainer), experimentalSyntaxEnabled);
        Analysis analyze = analyzer.analyze(statement);
        Plan plan = new LogicalPlanner(session, planOptimizers, new PlanNodeIdAllocator(), metadataManager).plan(analyze);

        SubPlan subplan = new PlanFragmenter().createSubPlans(plan);
        PlanFragment currentFragment = subplan.getFragment();

        AggregationQueryPlanVisitor visitor = new AggregationQueryPlanVisitor();
        currentFragment.getRoot().accept(visitor, null);

        for (SubPlan childPlan : subplan.getChildren()) {
            plan(childPlan, visitor);
        }

        Map<Symbol, Type> types = plan.getTypes();

        List<Symbol> fieldSymbols = visitor.fieldSymbols;
        List<String> fieldNames = visitor.fields;

        List<AggregationField> aggregationFields = visitor.aggregationFields.entrySet().stream()
                .filter(entry -> fieldSymbols.contains(entry.getKey()))
                .map(entry -> {
                    Symbol symbol = entry.getKey();
                    Signature signature = entry.getValue();
                    int idx = fieldSymbols.indexOf(symbol);

                    AccumulatorFactory bind = getAccumulatorFactory(signature);
                    return new AggregationField(idx, fieldNames.get(idx), bind.createAccumulator().getFinalType(), signature, bind);
                }).collect(Collectors.toList());

        List<GroupByField> groupByFields = visitor.fieldSymbols.stream()
                .filter(x -> !visitor.aggregationFields.containsKey(x))
                .map(symbol -> {
                    int idx = fieldSymbols.indexOf(symbol);
                    return new GroupByField(idx, fieldNames.get(idx), types.get(symbol));
                }).collect(Collectors.toList());

        boolean groupBy = !queryBody.getGroupBy().isEmpty();

        if(groupBy && groupByFields.isEmpty()) {
            throw new PrestoException(StandardErrorCode.INVALID_VIEW, "Group by queries must include at least one group by column.");
        }

        return new AggregationQuery(aggregationFields, groupByFields, queryBody.getOrderBy(),  queryBody.getLimit());
    }

    private void plan(SubPlan root, AggregationQueryPlanVisitor visitor)
    {
        PlanFragment currentFragment = root.getFragment();

        currentFragment.getRoot().accept(visitor, null);

        for (SubPlan childPlan : root.getChildren()) {
            plan(childPlan, visitor);
        }
    }

    public static class GroupByField {
        public final int position;
        public final Type colType;
        public final String colName;

        public GroupByField(int position, String colName, Type colType) {
            this.position = position;
            this.colType = colType;
            this.colName = colName;
        }
    }

    public static class AggregationField {
        public final int position;
        public final Type colType;
        public final String colName;
        public final Signature functionSignature;
        public final AccumulatorFactory accumulatorFactory;

        public AggregationField(int position, String colName, Type colType, Signature functionSignature, AccumulatorFactory accumulatorFactory) {
            this.position = position;
            this.colName = colName;
            this.colType = colType;
            this.functionSignature = functionSignature;
            this.accumulatorFactory = accumulatorFactory;
        }
    }
}
