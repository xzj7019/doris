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

import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.AbstractLogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.statistics.HistoricalPlanStatistics;
import org.apache.doris.statistics.HistoricalPlanStatisticsEntry;
import org.apache.doris.statistics.PlanStatistics;

import static com.google.common.hash.Hashing.sha256;
import static java.lang.Double.isNaN;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.List;
import java.util.Optional;

public class HistoryBasedPlanStatisticsUtil {

    public static Optional<Integer> getSimilarStatsIndex(
            HistoricalPlanStatistics historicalPlanStatistics,
            List<PlanStatistics> inputTableStatistics,
            double threshold, double hboRfSafeThreshold)
    {
        List<HistoricalPlanStatisticsEntry> lastRunsStatistics = historicalPlanStatistics.getLastRunsStatistics();

        if (lastRunsStatistics.isEmpty()) {
            return Optional.empty();
        }

        for (int lastRunsIndex = 0; lastRunsIndex < lastRunsStatistics.size(); ++lastRunsIndex) {
            if (inputTableStatistics.size() != lastRunsStatistics.get(lastRunsIndex).getInputTableStatistics().size()) {
                // This is not expected, but may happen when changing thrift definitions.
                continue;
            }
            boolean rowSimilarity = true;
            //boolean outputSizeSimilarity = true;

            // Match to historical stats only when size of input tables are similar to those of historical runs.
            for (int inputTablesIndex = 0; rowSimilarity && inputTablesIndex < inputTableStatistics.size(); ++inputTablesIndex) {
                PlanStatistics currentInputStatistics = inputTableStatistics.get(inputTablesIndex);
                PlanStatistics historicalInputStatistics = lastRunsStatistics.get(lastRunsIndex).getInputTableStatistics().get(inputTablesIndex);
                // check if rf safe
                boolean isRFSafe = historicalInputStatistics.isRuntimeFilterSafeNode(hboRfSafeThreshold);
                if (!isRFSafe) {
                    rowSimilarity = false;
                } else {
                    rowSimilarity = rowSimilarity && similarStats(currentInputStatistics.getOutputRows(),
                            historicalInputStatistics.getOutputRows(), threshold);
                }
                //outputSizeSimilarity = outputSizeSimilarity && similarStats(currentInputStatistics.getOutputSize().getValue(), historicalInputStatistics.getOutputSize().getValue(), threshold);
            }
            // Write information if both rows and output size are similar.
            if (rowSimilarity/* && outputSizeSimilarity*/) {
                return Optional.of(lastRunsIndex);
            }
        }
        return Optional.empty();
    }

    public static boolean similarStats(double stats1, double stats2, double threshold)
    {
        if (isNaN(stats1) && isNaN(stats2)) {
            return true;
        }
        return stats1 >= (1 - threshold) * stats2 && stats1 <= (1 + threshold) * stats2;
    }

    public static void collectScans(AbstractPlan planNode, List<LogicalOlapScan> scanList) {
        if (planNode instanceof LogicalOlapScan) {
            scanList.add((LogicalOlapScan) planNode);
        } else if (planNode instanceof GroupPlan
                && !((GroupPlan) planNode).getGroup().getLogicalExpressions().isEmpty()
                && ((GroupPlan) planNode).getGroup()
                .getLogicalExpressions().get(0).getPlan() instanceof AbstractLogicalPlan) {
            Plan logicalPlan = ((GroupPlan) planNode).getGroup().getLogicalExpressions().get(0).getPlan();
            collectScans((AbstractPlan) logicalPlan, scanList);
        } else if (planNode instanceof GroupPlan
                && ((GroupPlan) planNode).getGroup().getLogicalExpressions().isEmpty()
                && !((GroupPlan) planNode).getGroup().getPhysicalExpressions().isEmpty()
                && ((GroupPlan) planNode).getGroup()
                .getPhysicalExpressions().get(0).getPlan() instanceof AbstractPhysicalPlan) {
            Plan physicalPlan = ((GroupPlan) planNode).getGroup().getPhysicalExpressions().get(0).getPlan();
            collectScans((AbstractPlan) physicalPlan, scanList);
        } else {
            for (Object child : planNode.children()) {
                collectScans((AbstractPlan) child, scanList);
            }
        }
    }

    public static String hashCanonicalPlan(String planString) {
        return sha256().hashString(planString, UTF_8).toString();
    }

    public static Optional<HistoricalPlanStatisticsEntry> getSelectedHistoricalPlanStatisticsEntry(
            HistoricalPlanStatistics historicalPlanStatistics,
            List<PlanStatistics> inputTableStatistics,
            double historyMatchingThreshold,
            double hboRfSafeThreshold) {
        List<HistoricalPlanStatisticsEntry> lastRunsStatistics = historicalPlanStatistics.getLastRunsStatistics();
        if (lastRunsStatistics.isEmpty()) {
            return Optional.empty();
        }

        Optional<Integer> similarStatsIndex = getSimilarStatsIndex(historicalPlanStatistics,
                inputTableStatistics, historyMatchingThreshold, hboRfSafeThreshold);

        if (similarStatsIndex.isPresent()) {
            return Optional.of(lastRunsStatistics.get(similarStatsIndex.get()));
        }

        // TODO: Use linear regression to predict stats if we have only 1 table.
        return Optional.empty();
    }
}