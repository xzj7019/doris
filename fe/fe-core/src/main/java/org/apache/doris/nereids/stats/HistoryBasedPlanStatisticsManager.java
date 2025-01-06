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

import org.apache.doris.common.profile.ProfileManager;
import org.apache.doris.statistics.HistoryBasedPlanStatisticsProvider;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import static java.util.Objects.requireNonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HistoryBasedPlanStatisticsManager extends MasterDaemon {
{
    private static final Logger LOG = LogManager.getLogger(HistoryBasedPlanStatisticsManager.class);
    private static volatile HistoryBasedPlanStatisticsManager INSTANCE = null;


    private final HistoryBasedStatisticsCacheManager historyBasedStatisticsCacheManager;
    private final PlanCanonicalInfoProvider planCanonicalInfoProvider;
    private HistoryBasedPlanStatisticsProvider historyBasedPlanStatisticsProvider;

    public HistoryBasedPlanStatisticsManager()
    {
        this.historyBasedStatisticsCacheManager = new HistoryBasedStatisticsCacheManager();
        this.planCanonicalInfoProvider = new CachingPlanCanonicalInfoProvider(historyBasedStatisticsCacheManager, newObjectMapper, metadata);
    }

}