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

package com.amazon.opendistroforelasticsearch.ad.transport;

import java.time.Duration;
import java.time.Instant;
import java.util.Map.Entry;

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;

public class TransportState {
    private String detectorId;
    // detector definition and the definition fetch time
    private Entry<AnomalyDetector, Instant> detectorDef;
    // number of partitions and the number's fetch time
    private Entry<Integer, Instant> partitonNumber;
    // checkpoint fetch time
    private Instant checkpoint;
    // last detection error. Used by DetectorStateHandler to check if the error for a
    // detector has changed or not. If changed, trigger indexing.
    private Entry<String, Instant> lastDetectionError;
    // last training error. Used to save cold error by a concurrent cold start thread.
    private Entry<Exception, Instant> lastColdStartException;

    public TransportState(String detectorId) {
        this.detectorId = detectorId;
        detectorDef = null;
        partitonNumber = null;
        checkpoint = null;
        lastDetectionError = null;
        lastColdStartException = null;
    }

    public String getDetectorId() {
        return detectorId;
    }

    public Entry<AnomalyDetector, Instant> getDetectorDef() {
        return detectorDef;
    }

    public void setDetectorDef(Entry<AnomalyDetector, Instant> detectorDef) {
        this.detectorDef = detectorDef;
    }

    public Entry<Integer, Instant> getPartitonNumber() {
        return partitonNumber;
    }

    public void setPartitonNumber(Entry<Integer, Instant> partitonNumber) {
        this.partitonNumber = partitonNumber;
    }

    public Instant getCheckpoint() {
        return checkpoint;
    }

    public void setCheckpoint(Instant checkpoint) {
        this.checkpoint = checkpoint;
    };

    public Entry<String, Instant> getLastDetectionError() {
        return lastDetectionError;
    }

    public void setLastDetectionError(Entry<String, Instant> lastError) {
        this.lastDetectionError = lastError;
    }

    public Entry<Exception, Instant> getLastColdStartException() {
        return lastColdStartException;
    }

    public void setLastColdStartException(Entry<Exception, Instant> lastColdStartError) {
        this.lastColdStartException = lastColdStartError;
    }

    public boolean expired(Duration stateTtl, Instant now) {
        boolean ans = true;
        if (detectorDef != null) {
            ans = ans && expired(stateTtl, now, detectorDef.getValue());
        }
        if (partitonNumber != null) {
            ans = ans && expired(stateTtl, now, partitonNumber.getValue());
        }
        if (checkpoint != null) {
            ans = ans && expired(stateTtl, now, checkpoint);
        }
        if (lastDetectionError != null) {
            ans = ans && expired(stateTtl, now, lastDetectionError.getValue());
        }
        if (lastColdStartException != null) {
            ans = ans && expired(stateTtl, now, lastColdStartException.getValue());
        }
        return ans;
    }

    private boolean expired(Duration stateTtl, Instant now, Instant toCheck) {
        return toCheck.plus(stateTtl).isBefore(now);
    }
}
