/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.ad.ml.rcf;

import java.util.Objects;

/**
 * Data object containing combined RCF result.
 */
public class CombinedRcfResult {

    private final double score;
    private final double confidence;
    private final long totalUpdates;

    /**
     * Constructor with all arguments.
     *
     * @param score combined RCF score
     * @param confidence confidence of the score
     * @param totalUpdates max total updates made to all rcf partitions so far
     */
    public CombinedRcfResult(double score, double confidence, long totalUpdates) {
        this.score = score;
        this.confidence = confidence;
        this.totalUpdates = totalUpdates;
    }

    /**
     * Returns combined RCF score
     *
     * @return combined RCF score
     */
    public double getScore() {
        return score;
    }

    /**
     * Return confidence of the score.
     *
     * @return confidence of the score
     */
    public double getConfidence() {
        return confidence;
    }

    /**
     * Return max total updates made to all rcf partitions so far
     *
     * @return max total updates made to all rcf partitions so far
     */
    public long getTotalUpdates() {
        return totalUpdates;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        CombinedRcfResult that = (CombinedRcfResult) o;
        return Objects.equals(this.score, that.score)
            && Objects.equals(this.confidence, that.confidence)
            && Objects.equals(this.totalUpdates, that.totalUpdates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(score, confidence, totalUpdates);
    }
}
