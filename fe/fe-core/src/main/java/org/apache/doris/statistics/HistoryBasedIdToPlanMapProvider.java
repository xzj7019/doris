package org.apache.doris.statistics;

import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HistoryBasedIdToPlanMapProvider {
    private final Map<String, Map<Integer, PhysicalPlan>> cache = new ConcurrentHashMap<>();

    public HistoryBasedIdToPlanMapProvider() {}

    public Map<Integer, PhysicalPlan> getIdToPlanMap(String queryId) {
        return cache.getOrDefault(queryId, new ConcurrentHashMap<>());
    }

    public void putIdToPlanMap(String queryId, Map<Integer, PhysicalPlan> idToPlanMap) {
        cache.put(queryId, idToPlanMap);
    }

}