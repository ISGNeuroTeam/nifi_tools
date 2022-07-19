package com.isgneuro.nifi.tools.bloom;

import org.apache.spark.util.sketch.BloomFilter;
import org.apache.nifi.util.StopWatch;

import java.util.Set;


public class BloomWithStopWatch{
    BloomFilter bloomFilter;
    Set<String> tokens;
    StopWatch stopWatch;

    public BloomWithStopWatch(BloomFilter bloomFilter, StopWatch stopWatch) {
        this.bloomFilter = bloomFilter;
        this.stopWatch = stopWatch;
    }

    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    public void setBloomFilter(BloomFilter bloomFilter) {
        this.bloomFilter = bloomFilter;
    }

    public StopWatch getStopWatch() {
        return stopWatch;
    }

    public void setStopWatch(StopWatch stopWatch) {
        this.stopWatch = stopWatch;
    }
}
