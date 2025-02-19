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

package org.apache.doris.statistics;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TPlanNodeRuntimeStatsItem;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TablePlanStatistics extends PlanStatistics {
    //private final Literal lowerPartitionRangeBound;
    //private final Literal upperPartitionRangeBound;
    //private final Set<Literal> otherPredicateConstants;
    private ImmutableList<Long> selectedPartitionIds;
    private Set<Expression> partitionColumnPredicates = new HashSet<>();
    private Set<Expression> otherPredicate = new HashSet<>();
    private final Set<Expression> tableFilterSet;
    private final PartitionInfo partitionInfo;
    private final boolean isPartitionedTable;

    public TablePlanStatistics(int nodeId, long inputRows, long outputRows, long commonFilteredRows,
            long commonFilterInputRows, long runtimeFilteredRows, long runtimeFilterInputRows, long joinBuilderRows,
            long joinProbeRows, int joinBuilderSkewRatio, int joinProbeSkewRatio, int instanceNum,
            Set<Expression> tableFilterSet, boolean isPartitionedTable, PartitionInfo partitionInfo, List<Long> selectedPartitionIds) {
        super(nodeId, inputRows, outputRows, commonFilteredRows, commonFilterInputRows, runtimeFilteredRows,
                runtimeFilterInputRows, joinBuilderRows, joinProbeRows, joinBuilderSkewRatio, joinProbeSkewRatio,
                instanceNum);
        this.tableFilterSet = tableFilterSet;
        this.isPartitionedTable = isPartitionedTable;
        this.partitionInfo = partitionInfo;
        this.selectedPartitionIds = ImmutableList.copyOf(selectedPartitionIds);
        splitPartitionColumnPredicatesAndOthers();
    }

    private void splitPartitionColumnPredicatesAndOthers() {
        for (Expression expr : tableFilterSet) {
            Set<Slot> inputSlot = expr.getInputSlots();
            if (inputSlot.size() == 1 && inputSlot.iterator().next() instanceof SlotReference
                && ((SlotReference) inputSlot.iterator().next()).getColumn().isPresent()) {
                Column filterColumn = ((SlotReference) inputSlot.iterator().next()).getColumn().get();
                if (partitionInfo.getPartitionColumns().contains(filterColumn)) {
                    partitionColumnPredicates.add(expr);
                } else {
                    otherPredicate.add(expr);
                }
            } else {
                otherPredicate.add(expr);
            }
        }
    }

    public boolean hasSameOtherPredicates(TablePlanStatistics other) {
        return this.otherPredicate.containsAll(other.otherPredicate)
                && other.otherPredicate.containsAll(this.otherPredicate);
    }

    public boolean hasSamePartitionId(TablePlanStatistics other) {
        return this.selectedPartitionIds.equals(other.selectedPartitionIds);
    }

    public boolean isPartitionedTable() {
        return this.isPartitionedTable;
    }
}