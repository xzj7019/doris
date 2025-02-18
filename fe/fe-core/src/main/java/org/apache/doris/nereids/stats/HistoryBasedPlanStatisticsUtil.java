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
import org.apache.doris.statistics.TablePlanStatistics;

import static com.google.common.hash.Hashing.sha256;
import static java.lang.Double.isNaN;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.List;
import java.util.Optional;

public class HistoryBasedPlanStatisticsUtil {

    public static Optional<Integer> getAccurateStatsIndex(
            HistoricalPlanStatistics historicalPlanStatistics,
            List<PlanStatistics> inputTableStatistics,
            double rowThreshold,
            double hboRfSafeThreshold,
            boolean onlyMatchPartition)
    {
        List<HistoricalPlanStatisticsEntry> lastRunsStatistics = historicalPlanStatistics.getLastRunsStatistics();
        if (lastRunsStatistics.isEmpty()) {
            return Optional.empty();
        }

        for (int lastRunsIndex = 0; lastRunsIndex < lastRunsStatistics.size(); ++lastRunsIndex) {
            if (inputTableStatistics.size() != lastRunsStatistics.get(lastRunsIndex).getInputTableStatistics().size()) {
                continue;
            }
            boolean accurateMatch = true;
            for (int inputTablesIndex = 0; accurateMatch && inputTablesIndex < inputTableStatistics.size(); ++inputTablesIndex) {
                TablePlanStatistics currentInputStatistics = (TablePlanStatistics) inputTableStatistics.get(inputTablesIndex);
                TablePlanStatistics historicalInputStatistics = (TablePlanStatistics)  lastRunsStatistics.get(lastRunsIndex)
                        .getInputTableStatistics().get(inputTablesIndex);
                // check if rf safe
                boolean isRFSafe = historicalInputStatistics.isRuntimeFilterSafeNode(hboRfSafeThreshold);
                if (!isRFSafe) {
                    accurateMatch = false;
                } else {
                    // find the first full matching entry in lastRunEntries
                    accurateMatch = accurateMatch(currentInputStatistics, historicalInputStatistics, rowThreshold, onlyMatchPartition);
                }
            }
            if (accurateMatch) {
                return Optional.of(lastRunsIndex);
            }
        }
        return Optional.empty();
    }

    public static boolean accurateMatch(TablePlanStatistics currentInputStatistics,
            TablePlanStatistics historicalInputStatistics, double rowThreshold, boolean onlyMatchingPartition) {
        if (currentInputStatistics.isPartitionedTable() && historicalInputStatistics.isPartitionedTable()) {
            // for partition table, must ensure the pruned partition is the same
            // and the other predicate with the constant is the same
            boolean hasSamePartition = currentInputStatistics.hasSamePartitionId(historicalInputStatistics);
            boolean hasSameOtherPredicate = currentInputStatistics.hasSameOtherPredicates(historicalInputStatistics);
            // if onlyMatchingPartition is true, the matching condition will be
            // 1. the pruned partition ids are the same
            // 2. the row count threshold is in the threshold
            if (onlyMatchingPartition) {
                return hasSamePartition && similarStats(currentInputStatistics.getOutputRows(),
                        historicalInputStatistics.getOutputRows(), rowThreshold);
            } else {
                // if all predicates are the same, just return the accurate entry's index
                return hasSamePartition && hasSameOtherPredicate;
            }
        } else if (!currentInputStatistics.isPartitionedTable() && !historicalInputStatistics.isPartitionedTable()) {
            // for non-partition table, must ensure the other predicate with the constant is the same
            boolean hasSameOtherPredicate = currentInputStatistics.hasSameOtherPredicates(historicalInputStatistics);
            return hasSameOtherPredicate;
        } else {
            throw new RuntimeException("unexpected state during hbo input table stats matching");
        }
    }

    public static Optional<Integer> getSimilarStatsIndex(
            HistoricalPlanStatistics historicalPlanStatistics,
            List<PlanStatistics> inputTableStatistics,
            double rowThreshold, double hboRfSafeThreshold)
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
                    rowSimilarity = similarStats(currentInputStatistics.getOutputRows(),
                            historicalInputStatistics.getOutputRows(), rowThreshold);
                }
                //outputSizeSimilarity = outputSizeSimilarity
                // && similarStats(currentInputStatistics.getOutputSize().getValue(),
                // historicalInputStatistics.getOutputSize().getValue(), threshold);
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
            HistoricalPlanStatistics oldHistoricalPlanStatistics,
            List<PlanStatistics> inputTableStatistics,
            double historyMatchingThreshold,
            double hboRfSafeThreshold) {
        List<HistoricalPlanStatisticsEntry> lastRunsStatistics = oldHistoricalPlanStatistics.getLastRunsStatistics();
        if (lastRunsStatistics.isEmpty()) {
            return Optional.empty();
        }

        // firstly full matching, i.e, the same partition ids,
        //                             the same other predicate with the same constant
        // it is mainly for accurate matching under RETRY
        // by design, the accurate entry in the lastRunEntries will have only ONE entry
        Optional<Integer> accurateStatsIndex = HistoryBasedPlanStatisticsUtil.getAccurateStatsIndex(
                oldHistoricalPlanStatistics, inputTableStatistics, historyMatchingThreshold, hboRfSafeThreshold, false);
        if (accurateStatsIndex.isPresent()) {
            return Optional.of(lastRunsStatistics.get(accurateStatsIndex.get()));
        }

        Optional<Integer> accurateStatsOnlyMatchPartitionIndex = HistoryBasedPlanStatisticsUtil.getAccurateStatsIndex(
                oldHistoricalPlanStatistics, inputTableStatistics, historyMatchingThreshold, hboRfSafeThreshold, true);
        if (accurateStatsOnlyMatchPartitionIndex.isPresent()) {
            return Optional.of(lastRunsStatistics.get(accurateStatsOnlyMatchPartitionIndex.get()));
        }

        Optional<Integer> similarStatsIndex = HistoryBasedPlanStatisticsUtil.getSimilarStatsIndex(
                oldHistoricalPlanStatistics, inputTableStatistics, historyMatchingThreshold, hboRfSafeThreshold);
        if (similarStatsIndex.isPresent()) {
            return Optional.of(lastRunsStatistics.get(similarStatsIndex.get()));
        }
        // TODO: Use linear regression to predict stats if we have only 1 table.
        return Optional.empty();
    }
}