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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.properties.RequireProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPartitionTopN;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Implementation rule that convert logical partition-top-n to physical partition-top-n.
 */
public class LogicalPartitionTopNToPhysicalPartitionTopN extends OneImplementationRuleFactory {
    @Override
    public Rule build() {
        return logicalPartitionTopN().then(partitionTopN -> {
            List<OrderKey> orderKeys = !partitionTopN.getOrderKeys().isEmpty()
                    ? partitionTopN.getOrderKeys().stream()
                        .map(OrderExpression::getOrderKey)
                        .collect(ImmutableList.toImmutableList()) :
                    ImmutableList.of();

            RequireProperties requireAny = RequireProperties.of(PhysicalProperties.ANY);
            RequireProperties requireGather = RequireProperties.of(PhysicalProperties.GATHER);

            if (ConnectContext.get() == null || ConnectContext.get().getSessionVariable() == null
                    || !ConnectContext.get().getSessionVariable().isEnableTwoPhasePartitionTopn()) {
                PhysicalPartitionTopN<Plan> localPartitionTopN = new PhysicalPartitionTopN<>(
                        partitionTopN.getFunction(),
                        partitionTopN.getPartitionKeys(),
                        orderKeys,
                        partitionTopN.hasGlobalLimit(),
                        partitionTopN.getPartitionLimit(),
                        partitionTopN.getLogicalProperties(),
                        requireAny,
                        partitionTopN.child());

                return localPartitionTopN;
            } else {
                PhysicalPartitionTopN<Plan> anyLocalPartitionTopN = new PhysicalPartitionTopN<>(
                        partitionTopN.getFunction(),
                        partitionTopN.getPartitionKeys(),
                        orderKeys,
                        partitionTopN.hasGlobalLimit(),
                        partitionTopN.getPartitionLimit(),
                        partitionTopN.getLogicalProperties(),
                        requireAny,
                        partitionTopN.child());

                PhysicalPartitionTopN<Plan> anyLocalGatherGlobalPartitionTopN = new PhysicalPartitionTopN<>(
                        partitionTopN.getFunction(),
                        partitionTopN.getPartitionKeys(),
                        orderKeys,
                        partitionTopN.hasGlobalLimit(),
                        partitionTopN.getPartitionLimit(),
                        anyLocalPartitionTopN.getLogicalProperties(),
                        requireGather,
                        anyLocalPartitionTopN);

                RequireProperties requireHash = RequireProperties.of(
                        PhysicalProperties.createHash(partitionTopN.getPartitionKeys(), ShuffleType.REQUIRE));

                PhysicalPartitionTopN<Plan> anyLocalHashGlobalPartitionTopN = anyLocalGatherGlobalPartitionTopN
                        .withRequire(requireHash)
                        .withPartitionExpressions(partitionTopN.getPartitionKeys());

                return anyLocalHashGlobalPartitionTopN;
            }
        }).toRule(RuleType.LOGICAL_PARTITION_TOP_N_TO_PHYSICAL_PARTITION_TOP_N_RULE);
    }
}
