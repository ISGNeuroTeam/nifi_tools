package com.isgneuro.nifi.tools.bloom;

import org.apache.spark.util.sketch.BloomFilter;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class BloomFiltersInfo {
    private ConcurrentHashMap<String, BloomWithStopWatch> bloomFilters;
    private final long timeFrameMilliseconds;

    BloomFiltersInfo(long timeframeMilliseconds) {
        bloomFilters = new ConcurrentHashMap<>();
        this.timeFrameMilliseconds = timeframeMilliseconds;
    }

    Map<String, BloomWithTokens> getAll(){
        return bloomFilters.entrySet().stream().collect(Collectors.toMap(r -> r.getKey(), r-> r.getValue().getBloomWithTokens()));
    }

    Map<String, BloomWithTokens> getElapsed(){
        List<String> elapsed =  bloomFilters.entrySet().stream()
                .filter(w -> w.getValue().getStopWatch().getElapsed(TimeUnit.MILLISECONDS) >= timeFrameMilliseconds)
                .map(w -> w.getKey())
                .collect(Collectors.toList());
        Map<String, BloomWithTokens> res = elapsed.stream()
                .map(e ->{
                    BloomWithStopWatch bloomInfo = bloomFilters.remove(e);
                    return new AbstractMap.SimpleImmutableEntry<>(e, bloomInfo != null ?  bloomInfo.getBloomWithTokens() : null);
                })
                .filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        return res;
    }

    Boolean hasElapsed(){
        return bloomFilters.entrySet().stream()
                .filter(w -> w.getValue().getStopWatch().getElapsed(TimeUnit.MILLISECONDS) >= timeFrameMilliseconds)
                .count() > 0;
    }

    BloomWithStopWatch get(String id){
        return bloomFilters.get(id);
    }

    BloomWithStopWatch put(String id, BloomWithStopWatch bloomWithStopWatch){
        return bloomFilters.put(id, bloomWithStopWatch);
    }
}
