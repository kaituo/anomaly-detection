/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.ad.caching;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Random;

import org.junit.Before;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;
import com.amazon.opendistroforelasticsearch.ad.MemoryTracker;
import com.amazon.opendistroforelasticsearch.ad.ml.EntityModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager.ModelType;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelState;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.Entity;
import com.amazon.opendistroforelasticsearch.ad.ratelimit.CheckpointWriteQueue;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;

public class AbstractCacheTest extends AbstractADTest {
    protected String modelId1, modelId2, modelId3, modelId4;
    protected Entity entity1, entity2, entity3, entity4;
    protected ModelState<EntityModel> modelState1, modelState2, modelState3, modelState4;
    protected String detectorId;
    protected AnomalyDetector detector;
    protected Clock clock;
    protected Duration detectorDuration;
    protected float initialPriority;
    protected CacheBuffer cacheBuffer;
    protected long memoryPerEntity;
    protected MemoryTracker memoryTracker;
    protected CheckpointWriteQueue checkpointWriteQueue;
    protected Random random;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        modelId1 = "1";
        modelId2 = "2";
        modelId3 = "3";
        modelId4 = "4";

        detector = mock(AnomalyDetector.class);
        detectorId = "123";
        when(detector.getDetectorId()).thenReturn(detectorId);
        detectorDuration = Duration.ofMinutes(5);
        when(detector.getDetectionIntervalDuration()).thenReturn(detectorDuration);
        when(detector.getDetectorIntervalInSeconds()).thenReturn(detectorDuration.getSeconds());

        entity1 = Entity.createSingleAttributeEntity(detectorId, "attributeName1", "attributeVal1");
        entity2 = Entity.createSingleAttributeEntity(detectorId, "attributeName1", "attributeVal2");
        entity3 = Entity.createSingleAttributeEntity(detectorId, "attributeName1", "attributeVal3");
        entity4 = Entity.createSingleAttributeEntity(detectorId, "attributeName1", "attributeVal4");

        clock = mock(Clock.class);
        when(clock.instant()).thenReturn(Instant.now());

        memoryPerEntity = 81920;
        memoryTracker = mock(MemoryTracker.class);

        checkpointWriteQueue = mock(CheckpointWriteQueue.class);

        cacheBuffer = new CacheBuffer(
            1,
            1,
            memoryPerEntity,
            memoryTracker,
            clock,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            detectorId,
            checkpointWriteQueue,
            new Random(42)
        );

        initialPriority = cacheBuffer.getPriorityTracker().getUpdatedPriority(0);

        modelState1 = new ModelState<>(
            new EntityModel(entity1, new ArrayDeque<>(), null, null),
            modelId1,
            detectorId,
            ModelType.ENTITY.getName(),
            clock,
            0
        );

        modelState2 = new ModelState<>(
            new EntityModel(entity2, new ArrayDeque<>(), null, null),
            modelId2,
            detectorId,
            ModelType.ENTITY.getName(),
            clock,
            0
        );

        modelState3 = new ModelState<>(
            new EntityModel(entity3, new ArrayDeque<>(), null, null),
            modelId3,
            detectorId,
            ModelType.ENTITY.getName(),
            clock,
            0
        );

        modelState4 = new ModelState<>(
            new EntityModel(entity4, new ArrayDeque<>(), null, null),
            modelId4,
            detectorId,
            ModelType.ENTITY.getName(),
            clock,
            0
        );
    }
}
