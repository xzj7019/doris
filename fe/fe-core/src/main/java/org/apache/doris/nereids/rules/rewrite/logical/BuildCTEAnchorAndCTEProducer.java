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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;

/**
 * BuildCTEAnchorAndCTEProducer.
 */
public class BuildCTEAnchorAndCTEProducer extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalCTE().thenApply(ctx -> {
            return rewrite(ctx.root, ctx.cascadesContext);
        }).toRule(RuleType.BUILD_CTE_ANCHOR_AND_CTE_PRODUCER);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private LogicalPlan rewrite(LogicalPlan p, CascadesContext cascadesContext) {
        if (!(p instanceof LogicalCTE)) {
            return p;
        }
        LogicalCTE logicalCTE = (LogicalCTE) p;
        LogicalPlan child = (LogicalPlan) logicalCTE.child();
        for (int i = logicalCTE.getAliasQueries().size() - 1; i >= 0; i--) {
            LogicalSubQueryAlias s = (LogicalSubQueryAlias) logicalCTE.getAliasQueries().get(i);
            int id = logicalCTE.findUniqueId(s.getAlias());
            LogicalCTEProducer logicalCTEProducer = new LogicalCTEProducer(
                    rewrite((LogicalPlan) s.child(), cascadesContext), id);
            child = new LogicalCTEAnchor(logicalCTEProducer, child, id);
        }
        return child;
    }
}
