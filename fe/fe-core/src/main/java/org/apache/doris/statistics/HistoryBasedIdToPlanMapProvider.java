package org.apache.doris.statistics;

import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HistoryBasedIdToPlanMapProvider {
    private final Map<String, Map<Integer, PhysicalPlan>> idToPlanCache = new ConcurrentHashMap<>();

    private final Map<String, Map<PhysicalPlan, Integer>> planToIdCache = new ConcurrentHashMap<>();

    public HistoryBasedIdToPlanMapProvider() {}

    public Map<Integer, PhysicalPlan> getIdToPlanMap(String queryId) {
        return idToPlanCache.getOrDefault(queryId, new ConcurrentHashMap<>());
    }

    public void putIdToPlanMap(String queryId, Map<Integer, PhysicalPlan> idToPlanMap) {
        idToPlanCache.put(queryId, idToPlanMap);
    }

    public Map<PhysicalPlan, Integer> getPlanToIdMap(String queryId) {
        return planToIdCache.getOrDefault(queryId, new ConcurrentHashMap<>());
    }

    public void putPlanToIdMap(String queryId, Map<PhysicalPlan, Integer> idToPlanMap) {
        planToIdCache.put(queryId, idToPlanMap);
    }

}