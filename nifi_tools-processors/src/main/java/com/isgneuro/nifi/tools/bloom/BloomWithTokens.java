package com.isgneuro.nifi.tools.bloom;

import org.apache.nifi.util.StopWatch;
import org.apache.spark.util.sketch.BloomFilter;
import org.apache.spark.util.sketch.IncompatibleMergeException;

import java.util.HashSet;
import java.util.Set;

public class BloomWithTokens {
    private BloomFilter bloomFilter;
    private Set<String> bloomTokens;

    public BloomWithTokens(BloomFilter bloomFilter, Set<String> bloomTokens) {
        this.bloomFilter = bloomFilter;
        this.bloomTokens = bloomTokens;
        this.bloomTokens.forEach(this.bloomFilter::put);
    }
    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }
    public Set<String> getBloomTokens() {
        return bloomTokens;
    }

}
