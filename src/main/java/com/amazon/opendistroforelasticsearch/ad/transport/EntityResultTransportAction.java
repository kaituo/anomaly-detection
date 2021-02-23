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

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.NodeStateManager;
import com.amazon.opendistroforelasticsearch.ad.breaker.ADCircuitBreakerService;
import com.amazon.opendistroforelasticsearch.ad.caching.CacheProvider;
import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.EndRunException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.indices.ADIndex;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.ml.EntityColdStarter;
import com.amazon.opendistroforelasticsearch.ad.ml.EntityModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelState;
import com.amazon.opendistroforelasticsearch.ad.ml.ThresholdingResult;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.model.Entity;
import com.amazon.opendistroforelasticsearch.ad.ratelimit.CheckpointReadQueue;
import com.amazon.opendistroforelasticsearch.ad.ratelimit.ColdEntityQueue;
import com.amazon.opendistroforelasticsearch.ad.ratelimit.EntityFeatureRequest;
import com.amazon.opendistroforelasticsearch.ad.ratelimit.ResultWriteQueue;
import com.amazon.opendistroforelasticsearch.ad.ratelimit.ResultWriteRequest;
import com.amazon.opendistroforelasticsearch.ad.ratelimit.SegmentPriority;
import com.amazon.opendistroforelasticsearch.ad.util.ExceptionUtil;
import com.amazon.opendistroforelasticsearch.ad.util.ParseUtils;

public class EntityResultTransportAction extends HandledTransportAction<EntityResultRequest, AcknowledgedResponse> {

    private static final Logger LOG = LogManager.getLogger(EntityResultTransportAction.class);
    private ModelManager modelManager;
    private ADCircuitBreakerService adCircuitBreakerService;
    private CacheProvider cache;
    private final NodeStateManager stateManager;
    private AnomalyDetectionIndices indexUtil;
    private ResultWriteQueue resultWriteQueue;
    private CheckpointReadQueue checkpointReadQueue;
    private EntityColdStarter coldStarter;
    private ColdEntityQueue coldEntityQueue;
    private ThreadPool threadPool;

    @Inject
    public EntityResultTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        ModelManager manager,
        ADCircuitBreakerService adCircuitBreakerService,
        CacheProvider entityCache,
        NodeStateManager stateManager,
        AnomalyDetectionIndices indexUtil,
        ResultWriteQueue resultWriteQueue,
        CheckpointReadQueue checkpointReadQueue,
        EntityColdStarter coldStarer,
        ColdEntityQueue coldEntityQueue,
        ThreadPool threadPool
    ) {
        super(EntityResultAction.NAME, transportService, actionFilters, EntityResultRequest::new);
        this.modelManager = manager;
        this.adCircuitBreakerService = adCircuitBreakerService;
        this.cache = entityCache;
        this.stateManager = stateManager;
        this.indexUtil = indexUtil;
        this.resultWriteQueue = resultWriteQueue;
        this.checkpointReadQueue = checkpointReadQueue;
        this.coldStarter = coldStarer;
        this.coldEntityQueue = coldEntityQueue;
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, EntityResultRequest request, ActionListener<AcknowledgedResponse> listener) {
        if (adCircuitBreakerService.isOpen()) {
            threadPool.executor(AnomalyDetectorPlugin.AD_THREAD_POOL_NAME).execute(() -> cache.get().releaseMemoryForOpenCircuitBreaker());
            listener
                .onFailure(new LimitExceededException(request.getDetectorId(), CommonErrorMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG, false));
            return;
        }

        try {
            String detectorId = request.getDetectorId();

            Optional<AnomalyDetectionException> previousException = stateManager.fetchExceptionAndClear(detectorId);

            if (previousException.isPresent()) {
                Exception exception = previousException.get();
                LOG.error("Previous exception of {}: {}", detectorId, exception);
                if (exception instanceof EndRunException) {
                    EndRunException endRunException = (EndRunException) exception;
                    if (endRunException.isEndNow()) {
                        listener.onFailure(exception);
                        return;
                    }
                }

                listener = ExceptionUtil.wrapListener(listener, exception, detectorId);
            }

            stateManager.getAnomalyDetector(detectorId, onGetDetector(listener, detectorId, request, previousException));
        } catch (Exception exception) {
            LOG.error("fail to get entity's anomaly grade", exception);
            listener.onFailure(exception);
        }
    }

    private ActionListener<Optional<AnomalyDetector>> onGetDetector(
        ActionListener<AcknowledgedResponse> listener,
        String detectorId,
        EntityResultRequest request,
        Optional<AnomalyDetectionException> prevException
    ) {
        return ActionListener.wrap(detectorOptional -> {
            if (!detectorOptional.isPresent()) {
                listener.onFailure(new EndRunException(detectorId, "AnomalyDetector is not available.", true));
                return;
            }

            AnomalyDetector detector = detectorOptional.get();

            if (request.getEntities() == null) {
                listener.onResponse(null);
                return;
            }

            Instant executionStartTime = Instant.now();
            // Map<String, EntityFeatureRequest> cacheMissEntities = new HashMap<>();
            Map<Entity, double[]> cacheMissEntities = new HashMap<>();
            for (Entry<Entity, double[]> entityEntry : request.getEntities().entrySet()) {
                Entity categoricalValues = entityEntry.getKey();

                Optional<String> modelIdOptional = categoricalValues.getModelId(detectorId);
                if (false == modelIdOptional.isPresent()) {
                    continue;
                }

                String modelId = modelIdOptional.get();
                double[] datapoint = entityEntry.getValue();
                ModelState<EntityModel> entityModel = cache.get().get(modelId, detector);
                if (entityModel == null) {
                    // cache miss
                    cacheMissEntities.put(entityEntry.getKey(), entityEntry.getValue());
                    continue;
                }
                ThresholdingResult result = getAnomalyResultForEntity(datapoint, entityModel, modelId, detector, categoricalValues);
                // result.getRcfScore() = 0 means the model is not initialized
                // result.getGrade() = 0 means it is not an anomaly
                // So many EsRejectedExecutionException if we write no matter what
                if (result.getRcfScore() > 0) {
                    resultWriteQueue
                        .put(
                            new ResultWriteRequest(
                                System.currentTimeMillis() + detector.getDetectorIntervalInMilliseconds(),
                                detectorId,
                                result.getGrade() > 0 ? SegmentPriority.HIGH : SegmentPriority.MEDIUM,
                                new AnomalyResult(
                                    detectorId,
                                    result.getRcfScore(),
                                    result.getGrade(),
                                    result.getConfidence(),
                                    ParseUtils.getFeatureData(datapoint, detector),
                                    Instant.ofEpochMilli(request.getStart()),
                                    Instant.ofEpochMilli(request.getEnd()),
                                    executionStartTime,
                                    Instant.now(),
                                    null,
                                    categoricalValues,
                                    detector.getUser(),
                                    indexUtil.getSchemaVersion(ADIndex.RESULT)
                                )
                            )
                        );
                }
            }

            // split hot and cold entities
            Pair<List<Entity>, List<Entity>> hotColdEntities = cache
                .get()
                .selectUpdateCandidate(cacheMissEntities.keySet(), detectorId, detector);

            List<EntityFeatureRequest> hotEntityRequests = new ArrayList<>();
            List<EntityFeatureRequest> coldEntityRequests = new ArrayList<>();

            for (Entity hotEntity : hotColdEntities.getLeft()) {
                double[] hotEntityValue = cacheMissEntities.get(hotEntity);
                if (hotEntityValue == null) {
                    LOG.error(new ParameterizedMessage("feature value should not be null: [{}]", hotEntity));
                    continue;
                }
                hotEntityRequests
                    .add(
                        new EntityFeatureRequest(
                            System.currentTimeMillis() + detector.getDetectorIntervalInMilliseconds(),
                            detectorId,
                            // will change once we know the anomaly grade. Set it to low since most of the entities should not count as
                            // hot entities if the traffic is not totally random
                            SegmentPriority.MEDIUM,
                            hotEntity,
                            hotEntityValue,
                            request.getStart()
                        )
                    );
            }

            for (Entity coldEntity : hotColdEntities.getRight()) {
                double[] coldEntityValue = cacheMissEntities.get(coldEntity);
                if (coldEntityValue == null) {
                    LOG.error(new ParameterizedMessage("feature value should not be null: [{}]", coldEntity));
                    continue;
                }
                coldEntityRequests
                    .add(
                        new EntityFeatureRequest(
                            System.currentTimeMillis() + detector.getDetectorIntervalInMilliseconds(),
                            detectorId,
                            // will change once we know the anomaly grade. Set it to low since most of the entities should not count as
                            // hot entities if the traffic is not totally random
                            SegmentPriority.LOW,
                            coldEntity,
                            coldEntityValue,
                            request.getStart()
                        )
                    );
            }

            checkpointReadQueue.putAll(hotEntityRequests);
            coldEntityQueue.putAll(coldEntityRequests);

            // respond back
            if (prevException.isPresent()) {
                listener.onFailure(prevException.get());
            } else {
                listener.onResponse(new AcknowledgedResponse(true));
            }
        }, exception -> {
            LOG
                .error(
                    new ParameterizedMessage(
                        "fail to get entity's anomaly grade for detector [{}]: start: [{}], end: [{}]",
                        detectorId,
                        request.getStart(),
                        request.getEnd()
                    ),
                    exception
                );
            listener.onFailure(exception);
        });
    }

    /**
     * Compute anomaly result for the given data point
     * @param datapoint Data point
     * @param modelState the state associated with the entity
     * @param modelId the model Id
     * @param detector Detector accessor
     * @return anomaly result, confidence, and the corresponding RCF score.
     */
    ThresholdingResult getAnomalyResultForEntity(
        double[] datapoint,
        ModelState<EntityModel> modelState,
        String modelId,
        AnomalyDetector detector,
        Entity entity
    ) {
        if (modelState != null) {
            EntityModel entityModel = modelState.getModel();

            if (entityModel == null) {
                entityModel = new EntityModel(entity, new ArrayDeque<>(), null, null);
                modelState.setModel(entityModel);
            }

            if (entityModel.getRcf() == null || entityModel.getThreshold() == null) {
                coldStarter.trainModelFromExistingSamples(modelState);
            }

            if (entityModel.getRcf() != null && entityModel.getThreshold() != null) {
                return modelManager.score(datapoint, modelId, modelState);
            } else {
                entityModel.addSample(datapoint);
                return new ThresholdingResult(0, 0, 0);
            }
        } else {
            return new ThresholdingResult(0, 0, 0);
        }
    }
}
