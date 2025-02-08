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

import org.apache.doris.common.Config;
import org.apache.doris.common.ConfigBase.DefaultConfHandler;
import org.apache.doris.nereids.stats.HistoryBasedPlanStatisticsManager;
import org.apache.doris.planner.PlanNodeWithHash;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class InMemoryHistoryBasedPlanStatisticsProvider
        implements HistoryBasedPlanStatisticsProvider {
    private volatile Cache<String, HistoricalPlanStatistics> hboCache;
    public InMemoryHistoryBasedPlanStatisticsProvider() {
        hboCache = buildHboCaches(
                Config.hbo_cache_manage_num,
                Config.expire_hbo_cache_in_fe_second
        );
    }

    @Override
    public HistoricalPlanStatistics getHboStats(PlanNodeWithHash planNodeWithHash) {
        if (planNodeWithHash.getHash().isPresent()) {
             return hboCache.asMap().getOrDefault(planNodeWithHash.getHash().get(), HistoricalPlanStatistics.empty());
        }
        return HistoricalPlanStatistics.empty();
    }

    @Override
    public Map<PlanNodeWithHash, HistoricalPlanStatistics> getHboStats(List<PlanNodeWithHash> planNodeHashes) {
        return planNodeHashes.stream().collect(toImmutableMap(
                planNodeWithHash -> planNodeWithHash,
                planNodeWithHash -> {
                    if (planNodeWithHash.getHash().isPresent()) {
                        return hboCache.asMap().getOrDefault(planNodeWithHash.getHash().get(), HistoricalPlanStatistics.empty());
                    }
                    return HistoricalPlanStatistics.empty();
                }));
    }

    @Override
    public void putHboStats(Map<PlanNodeWithHash, HistoricalPlanStatistics> hashesStatisticsMap) {
        hashesStatisticsMap.forEach((planNodeWithHash, historicalPlanStatistics) -> {
            if (planNodeWithHash.getHash().isPresent()) {
                hboCache.put(planNodeWithHash.getHash().get(), historicalPlanStatistics);
            }
        });
    }

    private static Cache<String, HistoricalPlanStatistics> buildHboCaches(int hboCacheNum,
            long expireAfterAccessSeconds) {
        Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder()
                // auto evict cache when jvm memory too low
                .softValues();
        if (hboCacheNum > 0) {
            cacheBuilder.maximumSize(hboCacheNum);
        }
        if (expireAfterAccessSeconds > 0) {
            cacheBuilder = cacheBuilder.expireAfterAccess(Duration.ofSeconds(expireAfterAccessSeconds));
        }

        return cacheBuilder.build();
    }

    // NOTE: used in Config.sql_cache_manage_num.callbackClassString and
    //       Config.cache_last_version_interval_second.callbackClassString,
    //       don't remove it!
    public static class UpdateConfig extends DefaultConfHandler {
        @Override
        public void handle(Field field, String confVal) throws Exception {
            super.handle(field, confVal);
            InMemoryHistoryBasedPlanStatisticsProvider.updateConfig();
        }
    }

    public static synchronized void updateConfig() {
        HistoryBasedPlanStatisticsManager hboManger = HistoryBasedPlanStatisticsManager.getInstance();
        if (hboManger == null) {
            return;
        }
        HistoryBasedPlanStatisticsProvider hboProvider = hboManger.getHistoryBasedPlanStatisticsProvider();
        if (!(hboProvider instanceof InMemoryHistoryBasedPlanStatisticsProvider)) {
            return;
        }

        InMemoryHistoryBasedPlanStatisticsProvider inMemHboProvider =
                (InMemoryHistoryBasedPlanStatisticsProvider) hboProvider;

        Cache<String, HistoricalPlanStatistics> hboCaches = buildHboCaches(
                Config.sql_cache_manage_num,
                Config.expire_sql_cache_in_fe_second
        );
        hboCaches.putAll(inMemHboProvider.hboCache.asMap());
        inMemHboProvider.hboCache = hboCaches;
    }
}