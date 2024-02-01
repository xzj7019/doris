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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.properties.FdItem;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Eliminate outer join.
 */
public class EliminateGroupByKey extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate().then(agg -> {
            List<Expression> candiExprs = agg.getGroupByExpressions();
            // TODO: tracy code
            if (!(agg.child() instanceof LogicalProject) || candiExprs.size() != 3) {
                return null;
            }
            LogicalProject project = (LogicalProject) agg.child();

            List<Expression> rootExprs = new ArrayList<>();
            Set<Integer> rootExprsSet = new HashSet<>();

            List<FdItem> uniqueFdItems = new ArrayList<>();
            List<FdItem> nonUniqueFdItems = new ArrayList<>();
            ImmutableSet<FdItem> fdItems =  agg.child().child(0).getLogicalProperties().getFdItems();
            fdItems.stream().filter(e -> !e.isCandidate()).forEach( e-> {
                    if (((FdItem) e).isUnique()) {
                        uniqueFdItems.add(e);
                    } else {
                        nonUniqueFdItems.add(e);
                    }
                }
            );
            int minParentExprCnt = -1;
            ImmutableSet<NamedExpression> minParentExprs = ImmutableSet.of();
            for (int i = 0 ; i < uniqueFdItems.size(); i ++) {
                FdItem fdItem = uniqueFdItems.get(i);
                ImmutableSet<NamedExpression> parentExprs = fdItem.getParentExprs();
                if (minParentExprCnt == -1 || parentExprs.size() < minParentExprCnt) {
                    boolean isContain = isExprsContainFdParent(candiExprs, fdItem);
                    if (isContain) {
                        minParentExprCnt = parentExprs.size();
                        minParentExprs = ImmutableSet.copyOf(parentExprs);
                    }
                }
            }
            Set<Integer> eliminateSet = new HashSet<>();
            if (minParentExprs.size() > 0) {
                // TODO
            } else {
                for (int i = 0; i < nonUniqueFdItems.size(); i ++) {
                    FdItem fdItem = nonUniqueFdItems.get(i);
                    ImmutableSet<NamedExpression> parentExprs = fdItem.getParentExprs();
                    boolean isContains = isExprsContainFdParent(candiExprs, fdItem);
                    if (isContains) {
                        List<Expression> leftDomain = new ArrayList<>();
                        List<Expression> rightDomain = new ArrayList<>();
                        // generate new root exprs
                        for (int j = 0 ; j < rootExprs.size(); j ++) {
                            leftDomain.add(rootExprs.get(j));
                            boolean isInChild = fdItem.checkExprInChild(rootExprs.get(j), project);
                            if (isInChild) {
                                // root expr can be determined by other expr
                            } else {
                                rightDomain.add(rootExprs.get(j));
                            }
                        }
                        // todo:
                        for (int j = 0 ; j < parentExprs.size(); j ++) {
                            int index = findEqualExpr(candiExprs, parentExprs.asList().get(j));
                            if (index != -1) {
                                rightDomain.add(candiExprs.get(index));
                                if (eliminateSet.contains(index)) {
                                    // do nothing
                                } else {
                                    // fixme: remove the cast to Slot type
                                    leftDomain.add(candiExprs.get(index));
                                }
                            }
                        }
                        // check fd can eliminate new candi expr
                        for (int j = 0 ; j < candiExprs.size(); j ++) {
                            if (eliminateSet.contains(j)) {
                                // skip
                            } else {
                                // todo: remove cast from candi to Slot type
                                boolean isInChild = fdItem.checkExprInChild(candiExprs.get(j), project);
                                if (!isInChild) {
                                    // skip
                                } else {
                                    eliminateSet.add(j);
                                }
                            }
                        }
                        // if fd eliminate new candi exprs or new root exprs is smaller than the older,
                        // than use new root expr to replace old ones
                        List<Expression> newRootExprs = leftDomain.size() <= rightDomain.size() ?
                                leftDomain : rightDomain;
                        rootExprs.clear();
                        rootExprs.addAll(newRootExprs);
                    }
                }
            }
            // find the root expr, add into root exprs set, indicate the index in
            // candiExprs list
            for (int i = 0 ; i < rootExprs.size(); i ++) {
                // todo: add index valid checking
                int index = findEqualExpr(candiExprs, rootExprs.get(i));
                rootExprsSet.add(new Integer(index));
            }
            // other can't be determined expr, add into root exprs directly
            if (eliminateSet.size() < candiExprs.size()) {
                for (int i = 0; i < candiExprs.size(); i++) {
                    if (eliminateSet.contains(i)) {
                        // skip
                    } else {
                        rootExprsSet.add(i);
                    }
                }
            }
            rootExprs.clear();
            for (int i = 0; i < candiExprs.size(); i ++) {
                if (!rootExprsSet.contains(i)) {
                    // skip
                } else {
                    // fixme: remove the cast type
                    rootExprs.add((Slot) candiExprs.get(i));
                }
            }

            // use the new rootExprs as new group by keys
            List<NamedExpression> resultExprs = new ArrayList<>();
            for (int i = 0 ;i < rootExprs.size(); i ++) {
                resultExprs.add((NamedExpression) rootExprs.get(i));
            }


            // eliminate outputs keys
            List<NamedExpression> outputExprList = new ArrayList<>();
            for (int i = 0; i < agg.getOutputExpressions().size(); i++) {
                if (!rootExprsSet.contains(i)) {
                    // skip
                } else {
                    outputExprList.add(agg.getOutputExpressions().get(i));
                }
            }
            // todo: add agg column
            outputExprList.add(agg.getOutputExpressions().get(agg.getOutputExpressions().size() - 1));

            return new LogicalAggregate<>(rootExprs, outputExprList, agg.child());

        }).toRule(RuleType.ELIMINATE_GROUP_BY_KEY);
    }

    public int findEqualExpr(List<Expression> candiExprs, Expression expr) {
        for (int i = 0 ; i < candiExprs.size(); i ++) {
            if (candiExprs.get(i).equals(expr)) {
                return i;
            }
        }
        return -1;
    }

    private boolean isExprsContainFdParent(List<Expression> candiExprs, FdItem fdItem) {
        return fdItem.getParentExprs().stream().allMatch(e->candiExprs.contains(e));
    }
}
