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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.logical.AbstractLogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.planner.PlanNodeWithHash;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.HistoricalPlanStatistics;
import org.apache.doris.statistics.HistoricalPlanStatisticsEntry;
import org.apache.doris.statistics.HistoryBasedPlanStatisticsProvider;
import org.apache.doris.statistics.PlanStatistics;
import org.apache.doris.statistics.Statistics;
import com.google.common.collect.ImmutableList;

import com.google.common.collect.Maps;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    private Statistics getStatsFromHbo(AbstractPlan planNode, Statistics delegateStats) {
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
        Optional<List<PlanStatistics>> inputTableStatistics = getPlanNodeInputTableStatistics(planNode, true);
        // TODO: get current inputTableStatistics
        if (inputTableStatistics.isPresent() && !planStatistics.getLastRunsStatistics().isEmpty()) {
            double hboRfsafeThreshold = -1.0;
            if (cascadesContext.getConnectContext() != null
                    && cascadesContext.getConnectContext().getSessionVariable() != null) {
                hboRfsafeThreshold = cascadesContext.getConnectContext().getSessionVariable().getHboRfSafeThreshold();
            }
            Optional<HistoricalPlanStatisticsEntry> historicalPlanStatisticsEntry
                    = HistoryBasedPlanStatisticsUtil.getSelectedHistoricalPlanStatisticsEntry
                    (planStatistics, inputTableStatistics.get(), 0.1, hboRfsafeThreshold);
            if (historicalPlanStatisticsEntry.isPresent()) {
                PlanStatistics predictedPlanStatistics = historicalPlanStatisticsEntry.get().getPlanStatistics();
                // todo: choose which one is the output rows count
                delegateStats = delegateStats.withRowCountAndEnforceValid(predictedPlanStatistics.getOutputRows());
            }
        }
        return delegateStats;
    }

    private Optional<List<PlanStatistics>> getPlanNodeInputTableStatistics(AbstractPlan planNode, boolean cacheOnly)
    {
        ImmutableList.Builder<PlanStatistics> inputTableStatisticsBuilder = ImmutableList.builder();
        //List<LogicalOlapScan> scans = planNode.collectToList(LogicalOlapScan.class::isInstance);
        List<LogicalOlapScan> scans = new ArrayList<>();
        HistoryBasedPlanStatisticsUtil.collectScans(planNode, scans);
        for (LogicalOlapScan scan : scans) {
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
    }
}

