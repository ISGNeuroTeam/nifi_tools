package com.isgneuro.nifi.tools.bloom;

import org.apache.spark.util.sketch.BloomFilter;
import org.apache.nifi.util.StopWatch;

import java.util.HashSet;
import java.util.Set;


public class BloomWithStopWatch{
    private BloomWithTokens bloomWithTokens;
    private StopWatch stopWatch;

    public BloomWithStopWatch(BloomWithTokens bloomFilterWithTokens, StopWatch stopWatch) {
        this.bloomWithTokens = bloomFilterWithTokens;
        this.stopWatch = stopWatch;
    }
    public BloomWithTokens getBloomWithTokens() {
        return bloomWithTokens;
    }
    public StopWatch getStopWatch() {
        return stopWatch;
    }

}
