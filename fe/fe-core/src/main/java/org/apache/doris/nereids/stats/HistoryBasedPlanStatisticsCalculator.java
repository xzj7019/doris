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
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.logical.AbstractLogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.planner.PlanNodeWithHash;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.HistoricalPlanStatistics;
import org.apache.doris.statistics.HistoricalPlanStatisticsEntry;
import org.apache.doris.statistics.HistoryBasedPlanStatisticsProvider;
import org.apache.doris.statistics.PlanStatistics;
import org.apache.doris.statistics.Statistics;
import com.google.common.collect.ImmutableList;

import static java.util.Objects.requireNonNull;
import static org.apache.doris.common.profile.Profile.getSimilarStatsIndex;
import static com.google.common.hash.Hashing.sha256;
import static java.nio.charset.StandardCharsets.UTF_8;

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
        Statistics legacyStats = JoinEstimation.estimate(
                groupExpression.childStatistics(0),
                groupExpression.childStatistics(1), join);
        return getHistoricalStatistics((AbstractPlan) join, legacyStats);
    }

    private String hashCanonicalPlan(String planString)
    {
        return sha256().hashString(planString, UTF_8).toString();
    }

    private Statistics getHistoricalStatistics(AbstractPlan planNode, Statistics delegateStats) {
        String hash;
        if (planNode instanceof AbstractPhysicalPlan) {
            hash = planNode.hboTreeString();
            hash = hashCanonicalPlan(hash);
        } else if (planNode instanceof AbstractLogicalPlan) {
            hash = planNode.hboTreeString();
            hash = hashCanonicalPlan(hash);
        } else {
            throw new IllegalStateException("hbo get neither physical plan nor logical plan");
        }
        PlanNodeWithHash planNodeWithHash = new PlanNodeWithHash(planNode, Optional.of(hash));
        HistoricalPlanStatistics planStatistics = historyBasedPlanStatisticsProvider.getHboStats(planNodeWithHash);
        Optional<List<PlanStatistics>> inputTableStatistics = getPlanNodeInputTableStatistics(planNode, true);
        // TODO: get current inputTableStatistics
        if (inputTableStatistics.isPresent()) {
            Optional<HistoricalPlanStatisticsEntry> historicalPlanStatisticsEntry
                    = getSelectedHistoricalPlanStatisticsEntry
                    (planStatistics, inputTableStatistics.get(), 0.1);
            if (historicalPlanStatisticsEntry.isPresent()) {
                PlanStatistics predictedPlanStatistics = historicalPlanStatisticsEntry.get().getPlanStatistics();
                // todo: choose which one is the output rows count
                delegateStats = delegateStats.withRowCountAndEnforceValid(predictedPlanStatistics.getOutputRows());
            }
        }
        return delegateStats;
    }

    public static void collectScans(AbstractPlan planNode, List<LogicalOlapScan> scanList) {
        if (planNode instanceof LogicalOlapScan) {
            scanList.add((LogicalOlapScan) planNode);
        } else if (planNode instanceof GroupPlan && ((GroupPlan) planNode).getGroup()
                .getLogicalExpressions().get(0).getPlan() instanceof LogicalPlan) {
            Plan logicalPlan = ((GroupPlan) planNode).getGroup().getLogicalExpressions().get(0).getPlan();
            collectScans((AbstractPlan) logicalPlan, scanList);
        } else {
            for (Object child : planNode.children()) {
                collectScans((AbstractPlan) child, scanList);
            }
        }
    }

    private Optional<List<PlanStatistics>> getPlanNodeInputTableStatistics(AbstractPlan planNode, boolean cacheOnly)
    {
        ImmutableList.Builder<PlanStatistics> inputTableStatisticsBuilder = ImmutableList.builder();
        //List<LogicalOlapScan> scans = planNode.collectToList(LogicalOlapScan.class::isInstance);
        List<LogicalOlapScan> scans = new ArrayList<>();
        collectScans(planNode, scans);
        for (LogicalOlapScan scan : scans) {
            String hash = scan.hboTreeString();
            hash = hashCanonicalPlan(hash);
            PlanNodeWithHash planNodeWithHash = new PlanNodeWithHash(scan, Optional.of(hash));
            HistoricalPlanStatistics historicalPlanStatistics = historyBasedPlanStatisticsProvider
                    .getHboStats(planNodeWithHash);
            if (historicalPlanStatistics.equals(historicalPlanStatistics.empty())) {
                return Optional.empty();
            } else {
                PlanStatistics planStatistics = historicalPlanStatistics.getLastRunsStatistics()
                        .get(0).getPlanStatistics();
                inputTableStatisticsBuilder.add(planStatistics);
            }
        }

        return Optional.of(inputTableStatisticsBuilder.build());
    }

    public static Optional<HistoricalPlanStatisticsEntry> getSelectedHistoricalPlanStatisticsEntry(
            HistoricalPlanStatistics historicalPlanStatistics,
            List<PlanStatistics> inputTableStatistics,
            double historyMatchingThreshold) {
        List<HistoricalPlanStatisticsEntry> lastRunsStatistics = historicalPlanStatistics.getLastRunsStatistics();
        if (lastRunsStatistics.isEmpty()) {
            return Optional.empty();
        }

        Optional<Integer> similarStatsIndex = getSimilarStatsIndex(historicalPlanStatistics,
                inputTableStatistics, historyMatchingThreshold);

        if (similarStatsIndex.isPresent()) {
            return Optional.of(lastRunsStatistics.get(similarStatsIndex.get()));
        }

        // TODO: Use linear regression to predict stats if we have only 1 table.
        return Optional.empty();
    }
}

