// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.stats;

import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.algebra.OlapScan;
import org.apache.doris.nereids.trees.plans.logical.AbstractLogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.planner.PlanNodeWithHash;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.HistoricalPlanStatistics;
import org.apache.doris.statistics.HistoricalPlanStatisticsEntry;
import org.apache.doris.statistics.HistoryBasedIdToPlanMapProvider;
import org.apache.doris.statistics.HistoryBasedPlanStatisticsProvider;
import org.apache.doris.statistics.InMemoryHistoryBasedPlanStatisticsProvider;
import org.apache.doris.statistics.PlanStatistics;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.TablePlanStatistics;

import com.google.common.collect.ImmutableList;

import com.google.common.collect.Maps;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * HistoryBasedPlanStatisticsCalculator
 */
public class HistoryBasedPlanStatisticsCalculator extends StatsCalculator {
    private final HistoryBasedPlanStatisticsProvider historyBasedPlanStatisticsProvider;
    public HistoryBasedPlanStatisticsCalculator(GroupExpression groupExpression, boolean forbidUnknownColStats,
            Map<String, ColumnStatistic> columnStatisticMap, boolean isPlayNereidsDump,
            Map<CTEId, Statistics> cteIdToStats, CascadesContext context) {
        super(groupExpression, forbidUnknownColStats, columnStatisticMap, isPlayNereidsDump,
                cteIdToStats, context);
        this.historyBasedPlanStatisticsProvider = requireNonNull(HistoryBasedPlanStatisticsManager.getInstance()
                        .getHistoryBasedPlanStatisticsProvider(), "historyBasedPlanStatisticsProvider is null");
    }

    @Override
    public void estimate() {
        super.estimate();
    }

    // NOTE: can't override computeScan since the publishing side's plan hash of scan node
    // use the scan's hbo string but embedding the filter info into the input table structure.
    // if the matching logic here want to support filter node's hbo info's reusing, it only needs
    // to hook the computeFilter and use original scan's plan hash string, and also embedding the
    // parent filter info into the scan node also.
    @Override
    protected Statistics computeFilter(Filter filter) {
        Statistics legacyStats = super.computeFilter(filter);
        boolean isLogicalFilterOnTs = isLogicalFilterOnLogicalScan(filter);
        boolean isPhysicalFilterOnTs = isPhysicalFilterOnPhysicalScan(filter);
        if (isLogicalFilterOnTs || isPhysicalFilterOnTs) {
            return getStatsFromHbo((AbstractPlan) filter, legacyStats);
        } else {
            return legacyStats;
        }
    }

    @Override
    protected Statistics computeJoin(Join join) {
        Statistics legacyStats = super.computeJoin(join);
        return getStatsFromHbo((AbstractPlan) join, legacyStats);
    }

    @Override
    protected Statistics computeAggregate(Aggregate<? extends Plan> aggregate) {
        Statistics legacyStats = super.computeAggregate(aggregate);
        return getStatsFromHbo((AbstractPlan) aggregate, legacyStats);
    }

    private boolean isLogicalFilterOnLogicalScan(Filter filter) {
        if (filter instanceof LogicalFilter
            && ((LogicalFilter) filter).child() instanceof GroupPlan
            && ((GroupPlan) ((LogicalFilter) filter).child()).getGroup() != null
            && !((GroupPlan) ((LogicalFilter) filter).child()).getGroup().getLogicalExpressions().isEmpty()
            && ((GroupPlan) ((LogicalFilter) filter).child()).getGroup().getLogicalExpressions().get(0).getPlan() instanceof LogicalOlapScan) {
            return true;
        } else {
            return false;
        }
    }

    private boolean isPhysicalFilterOnPhysicalScan(Filter filter) {
        if (filter instanceof PhysicalFilter
                && ((PhysicalFilter) filter).child() instanceof GroupPlan
                && ((GroupPlan) ((PhysicalFilter) filter).child()).getGroup() != null
                && !((GroupPlan) ((PhysicalFilter) filter).child()).getGroup().getPhysicalExpressions().isEmpty()
                && ((GroupPlan) ((PhysicalFilter) filter).child()).getGroup().getPhysicalExpressions().get(0).getPlan() instanceof PhysicalOlapScan) {
            return true;
        } else {
            return false;
        }
    }

    private Statistics getStatsFromHbo(AbstractPlan planNode, Statistics delegateStats) {
        boolean isFilterOnTs = false;
        AbstractPlan originalPlanNode = planNode;
        if (planNode instanceof Filter) {
            // handle filter to access scan, whose child is scan which ensured before
            if (isLogicalFilterOnLogicalScan((Filter) planNode)) {
                planNode = (LogicalOlapScan) ((GroupPlan) ((LogicalFilter) planNode).child())
                        .getGroup().getLogicalExpressions().get(0).getPlan();
                isFilterOnTs = true;
            } else if (isPhysicalFilterOnPhysicalScan((Filter) planNode)) {
                planNode = (PhysicalOlapScan) ((GroupPlan) ((PhysicalFilter) planNode).child())
                        .getGroup().getPhysicalExpressions().get(0).getPlan();
                isFilterOnTs = true;
            } else {
                throw new AnalysisException("unexpected filter type");
            }
        }
        String hash;
        if (planNode instanceof AbstractPhysicalPlan) {
            hash = planNode.hboTreeString();
            hash = HistoryBasedPlanStatisticsUtil.hashCanonicalPlan(hash);
        } else if (planNode instanceof AbstractLogicalPlan) {
            hash = planNode.hboTreeString();
            hash = HistoryBasedPlanStatisticsUtil.hashCanonicalPlan(hash);
        } else {
            throw new IllegalStateException("hbo get neither physical plan nor logical plan");
        }
        PlanNodeWithHash planNodeWithHash = new PlanNodeWithHash(planNode, Optional.of(hash));
        HistoricalPlanStatistics planStatistics = historyBasedPlanStatisticsProvider.getHboStats(planNodeWithHash);
        // TODO: get current inputTableStatistics
        // use the following getPlanNodeInputTableStatistics may lead a problem
        // case1: select xxx from t where c1 = 1 vs. select xxx from t where c1 = 1 and c2 = 2
        // since the input table t will have two entries in lastRunEntries list
        // if the getPlanNodeInputTableStatistics only find the table entry by the table name, etc
        // it may find the wrong entry for the different filter pattern
        // by contract, use current planStatistics is safe because the plan hash has ensure the plan hbo string
        // is matched, although the detailed constant may note be same
        if (!planStatistics.getLastRunsStatistics().isEmpty()) {
            // TODO: the currentInputTableStatistics is the mocked one which needs to be updated with current filter, etc
            // use entry 0 or the last entry will be considered
            // entry 0: the oldest entry
            // entry last: the newest entry
            int selectedIndex = planStatistics.getLastRunsStatistics().size() - 1;
            double hboRfsafeThreshold = -1.0;
            double rowCountMatchingThreshold = 0.1;
            boolean isEnableHboNonStrictMatchingMode = false;
            if (cascadesContext.getConnectContext() != null
                    && cascadesContext.getConnectContext().getSessionVariable() != null) {
                hboRfsafeThreshold = cascadesContext.getConnectContext().getSessionVariable()
                        .getHboRfSafeThreshold();
                rowCountMatchingThreshold = cascadesContext.getConnectContext().getSessionVariable()
                        .getHboRowMatchingThreshold();
                isEnableHboNonStrictMatchingMode = cascadesContext.getConnectContext().getSessionVariable()
                        .isEnableHboNonStrictMatchingMode();
                if (isEnableHboNonStrictMatchingMode) {
                    // TODO: FIX this
                    selectedIndex = 0;
                }
            }
            List<PlanStatistics> currentInputTableStatistics = planStatistics.getLastRunsStatistics().get(selectedIndex).getInputTableStatistics();
            if (!currentInputTableStatistics.isEmpty()) {
                Optional<List<PlanStatistics>> inputTableStatistics = getPlanNodeInputTableStatistics(currentInputTableStatistics, true);
                if (inputTableStatistics.isPresent()) {
                    //if (!planStatistics.getLastRunsStatistics().isEmpty() && inputTableStatistics.isPresent()) {
                    // FIXME: always get 0 will be wrong
                    // since the existing cache always has entry 0 and it will always hit entry 0 all the time
                    // TODO: try to use the last entry as a replacement, refer updatePlanStatistics(latest insertion as the last index)
                    //Optional<List<PlanStatistics>> currentInputTableStatistics = Optional.of(planStatistics
                    //        .getLastRunsStatistics().get(0).getInputTableStatistics());
                    // NOTE: must update the partition and common filter at input plan statistics
                    // in order to match the filter(but actually a TablePlanStatistics) entry in the hbo cache
                    // i.e, logical filter node is mapped to TablePlanStatistics in hbo cache(important!!!)
                    //if (currentInputTableStatistics.isPresent()) {
                    // extract filters info out to update inputTableStatistics as a current search key
                    //List<PlanStatistics> inputTableStatistics = currentInputTableStatistics.get();
                    // TODO: for join node, it will update the filter info for the input plan statistics
                    // it will find the wrong cache entry
                    //if (isFilterOnTs) {
                    //    inputTableStatistics = updateCurrentInputTableStatisticsWithFilter(inputTableStatistics,
                    //            (Filter) originalPlanNode, (OlapScan) planNode);
                    //} else {
                    //}
                    Optional<HistoricalPlanStatisticsEntry> historicalPlanStatisticsEntry
                            = HistoryBasedPlanStatisticsUtil.getSelectedHistoricalPlanStatisticsEntry
                            (planStatistics, inputTableStatistics.get(), rowCountMatchingThreshold, hboRfsafeThreshold, isEnableHboNonStrictMatchingMode);
                    if (historicalPlanStatisticsEntry.isPresent()) {
                        PlanStatistics predictedPlanStatistics = historicalPlanStatisticsEntry.get()
                                .getPlanStatistics();
                        // todo: choose which one is the output rows count
                        delegateStats = delegateStats.withRowCountAndEnforceValid(
                                predictedPlanStatistics.getOutputRows());
                    }
                }
            }
        }
        return delegateStats;
    }

    /*
    private List<PlanStatistics> updateCurrentInputTableStatisticsByTableExprMap(List<PlanStatistics> currentInputTableStatistics) {
        HistoryBasedPlanStatisticsManager hboManager = HistoryBasedPlanStatisticsManager.getInstance();
        HistoryBasedIdToPlanMapProvider idToMapProvider = hboManager.getHistoryBasedIdToPlanMapProvider();
        String queryId = DebugUtil.printId(cascadesContext.getConnectContext().queryId());
        Map<TableIf, Set<Expression>> tableToExprMap = idToMapProvider.getTableToExprMap(queryId);
    }

    private List<PlanStatistics> updateCurrentInputTableStatisticsWithFilter(List<PlanStatistics> currentInputTableStatistics,
            Filter filter, OlapScan scan) {
        if (currentInputTableStatistics.size() != 1) {
            throw new AnalysisException("unexpected status that filter's inputPlanStatistics doesn't have 1 entry");
        }
        ImmutableList.Builder<PlanStatistics> outputTableStatisticsBuilder = ImmutableList.builder();
        Set<Expression> tableFilterSet = filter.getConjuncts();
        PlanStatistics inputPlanStatistics = currentInputTableStatistics.get(0);
        TablePlanStatistics newInputPlanStatistics = new TablePlanStatistics(inputPlanStatistics, tableFilterSet,
                scan.getTable().isPartitionedTable(), scan.getTable().getPartitionInfo(), scan.getSelectedPartitionIds());
        outputTableStatisticsBuilder.add(newInputPlanStatistics);
        return outputTableStatisticsBuilder.build();
    }*/

    private Optional<List<PlanStatistics>> getPlanNodeInputTableStatistics(
            List<PlanStatistics> currentInputTableStatistics, boolean cacheOnly) {
        HistoryBasedPlanStatisticsManager hboManager = HistoryBasedPlanStatisticsManager.getInstance();
        HistoryBasedIdToPlanMapProvider idToMapProvider = hboManager.getHistoryBasedIdToPlanMapProvider();

        String queryId = DebugUtil.printId(cascadesContext.getConnectContext().queryId());
        Map<TableIf, Set<Expression>> tableToExprMap = idToMapProvider.getTableToExprMap(queryId);
        // FIXME: current queryId's idToPlanMap is NOT available NOW
        //Map<Integer, PhysicalPlan> idToPlanMap = idToMapProvider.getIdToPlanMap(queryId);
        ImmutableList.Builder<PlanStatistics> outputTableStatisticsBuilder = ImmutableList.builder();

        for (PlanStatistics inputTableStatistics : currentInputTableStatistics) {
            //int tableNodeId = inputTableStatistics.getNodeId();
            //PhysicalPlan planNode = idToPlanMap.get(tableNodeId);
            //if (!(planNode instanceof PhysicalOlapScan)) {
            //    throw new RuntimeException("unexpected plan node type");
            //}
            PhysicalOlapScan tableScan = ((TablePlanStatistics) inputTableStatistics).getTable();
            Set<Expression> tableFilterSet = tableToExprMap.get(tableScan.getTable());

            // here is the assumption that the table is always same with different table id(TODO: verify this)
            TablePlanStatistics newInputPlanStatistics = new TablePlanStatistics(inputTableStatistics, tableScan, tableFilterSet,
                    tableScan.getTable().isPartitionedTable(), tableScan.getTable().getPartitionInfo(),
                    tableScan.getSelectedPartitionIds());
            outputTableStatisticsBuilder.add(newInputPlanStatistics);
        }
        return Optional.of(outputTableStatisticsBuilder.build());
    }

    /*
    private Optional<List<PlanStatistics>> getPlanNodeInputTableStatistics(AbstractPlan planNode,
                List<PlanStatistics> currentInputTableStatistics, boolean cacheOnly) {
        ImmutableList.Builder<PlanStatistics> inputTableStatisticsBuilder = ImmutableList.builder();
        Set<LogicalOlapScan> scans = new HashSet<>();
        // TODO: cbo stage may miss some scan during memo tree visiting, use the entry 0 instead + replacement
        HistoryBasedPlanStatisticsUtil.collectScans(planNode, scans);
        for (LogicalOlapScan scan : scans) {
            // FIXME: logical scan not contains filter info and can't match the physical filter's info
            // consider the case that date_dim first with d_moy = 7 but the second not, it will find the entry 0
            // but correct entry is 1
            String hash = scan.hboTreeString();
            hash = HistoryBasedPlanStatisticsUtil.hashCanonicalPlan(hash);
            PlanNodeWithHash planNodeWithHash = new PlanNodeWithHash(scan, Optional.of(hash));
            HistoricalPlanStatistics historicalPlanStatistics = historyBasedPlanStatisticsProvider
                    .getHboStats(planNodeWithHash);
            if (historicalPlanStatistics.equals(historicalPlanStatistics.empty())) {
                return Optional.empty();
            } else {
                // TODO: first match checking based on accurate partition info
                // otherwise, use the entry 0 since the param number has been considered in plan hash
                // note: next round, the accurate matching entry will be added into and will be matched next time (TODO: testing)
                PlanStatistics planStatistics = historicalPlanStatistics.getLastRunsStatistics()
                        .get(0).getPlanStatistics();
                inputTableStatisticsBuilder.add(planStatistics);
            }
        }

        return Optional.of(inputTableStatisticsBuilder.build());
    }*/
}

