package com.amazon.opendistroforelasticsearch.ad.util;

public interface Mergeable {
    public void merge(Mergeable other);
}
