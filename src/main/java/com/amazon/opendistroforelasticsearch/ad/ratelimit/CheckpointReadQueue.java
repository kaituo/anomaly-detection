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

package com.amazon.opendistroforelasticsearch.ad.ratelimit;

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.CHECKPOINT_READ_QUEUE_BATCH_SIZE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.CHECKPOINT_READ_QUEUE_CONCURRENCY;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.NodeStateManager;
import com.amazon.opendistroforelasticsearch.ad.breaker.ADCircuitBreakerService;
import com.amazon.opendistroforelasticsearch.ad.caching.CacheProvider;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.indices.ADIndex;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.ml.CheckpointDao;
import com.amazon.opendistroforelasticsearch.ad.ml.EntityColdStarter;
import com.amazon.opendistroforelasticsearch.ad.ml.EntityModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager.ModelType;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelState;
import com.amazon.opendistroforelasticsearch.ad.ml.ThresholdingResult;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.model.Entity;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.ExceptionUtil;
import com.amazon.opendistroforelasticsearch.ad.util.ParseUtils;

public class CheckpointReadQueue extends BatchQueue<EntityFeatureRequest, MultiGetRequest, MultiGetResponse> {
    private static final Logger LOG = LogManager.getLogger(CheckpointReadQueue.class);
    private final ModelManager modelManager;
    private final CheckpointDao checkpointDao;
    private final EntityColdStartQueue entityColdStartQueue;
    private final ResultWriteQueue resultWriteQueue;
    private final AnomalyDetectionIndices indexUtil;
    private final CacheProvider cacheProvider;
    private final CheckpointWriteQueue checkpointWriteQueue;
    private final EntityColdStarter entityColdStarter;

    public CheckpointReadQueue(
        long heapSizeInBytes,
        int singleRequestSizeInBytes,
        Setting<Float> maxHeapPercentForQueueSetting,
        ClusterService clusterService,
        Random random,
        ADCircuitBreakerService adCircuitBreakerService,
        ThreadPool threadPool,
        Settings settings,
        float maxQueuedTaskRatio,
        Clock clock,
        float mediumSegmentPruneRatio,
        float lowSegmentPruneRatio,
        int maintenanceFreqConstant,
        ClientUtil clientUtil,
        Duration executionTtl,
        ModelManager modelManager,
        CheckpointDao checkpointDao,
        EntityColdStartQueue entityColdStartQueue,
        ResultWriteQueue resultWriteQueue,
        NodeStateManager stateManager,
        AnomalyDetectionIndices indexUtil,
        CacheProvider cacheProvider,
        Duration stateTtl,
        CheckpointWriteQueue checkpointWriteQueue,
        EntityColdStarter entityColdStarter
    ) {
        super(
            "checkpoint-read",
            heapSizeInBytes,
            singleRequestSizeInBytes,
            maxHeapPercentForQueueSetting,
            clusterService,
            random,
            adCircuitBreakerService,
            threadPool,
            settings,
            maxQueuedTaskRatio,
            clock,
            mediumSegmentPruneRatio,
            lowSegmentPruneRatio,
            maintenanceFreqConstant,
            clientUtil,
            CHECKPOINT_READ_QUEUE_CONCURRENCY,
            executionTtl,
            CHECKPOINT_READ_QUEUE_BATCH_SIZE,
            stateTtl,
            stateManager
        );

        this.modelManager = modelManager;
        this.checkpointDao = checkpointDao;
        this.entityColdStartQueue = entityColdStartQueue;
        this.resultWriteQueue = resultWriteQueue;
        this.indexUtil = indexUtil;
        this.cacheProvider = cacheProvider;
        this.checkpointWriteQueue = checkpointWriteQueue;
        this.entityColdStarter = entityColdStarter;
    }

    @Override
    protected void executeBatchRequest(MultiGetRequest request, ActionListener<MultiGetResponse> listener) {
        clientUtil.<MultiGetRequest, MultiGetResponse>execute(MultiGetAction.INSTANCE, request, listener);
    }

    @Override
    protected MultiGetRequest toBatchRequest(List<EntityFeatureRequest> toProcess) {
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        for (EntityRequest request : toProcess) {
            Optional<String> modelId = request.getModelId();
            if (false == modelId.isPresent()) {
                continue;
            }
            multiGetRequest.add(new MultiGetRequest.Item(CommonName.CHECKPOINT_INDEX_NAME, modelId.get()));
        }
        return multiGetRequest;
    }

    @Override
    protected ActionListener<MultiGetResponse> getResponseListener(List<EntityFeatureRequest> toProcess, MultiGetRequest batchRequest) {
        return ActionListener.wrap(response -> {
            final MultiGetItemResponse[] itemResponses = response.getResponses();
            Map<String, MultiGetItemResponse> successfulRequests = new HashMap<>();

            // lazy init since we don't expect retryable requests to happen often
            Set<String> retryableRequests = null;
            Set<String> notFoundModels = null;
            for (MultiGetItemResponse itemResponse : itemResponses) {
                String modelId = itemResponse.getId();
                if (itemResponse.isFailed()) {
                    final Exception failure = itemResponse.getFailure().getFailure();
                    if (failure instanceof IndexNotFoundException) {
                        for (EntityRequest origRequest : toProcess) {
                            // submit to cold start queue
                            entityColdStartQueue.put(origRequest);
                        }
                        return;
                    } else if (ExceptionUtil.isNotFound(failure)) {
                        if (notFoundModels == null) {
                            notFoundModels = new HashSet<>();
                        }
                        notFoundModels.add(modelId);
                    } else if (ExceptionUtil.isRetryAble(failure)) {
                        if (retryableRequests == null) {
                            retryableRequests = new HashSet<>();
                        }
                        retryableRequests.add(modelId);
                    } else {
                        LOG.info("Unexpected failure", failure);
                    }
                } else {
                    successfulRequests.put(modelId, itemResponse);
                }
            }

            // deal with not found model
            if (notFoundModels != null) {
                for (EntityRequest origRequest : toProcess) {
                    Optional<String> modelId = origRequest.getModelId();
                    if (modelId.isPresent() && notFoundModels.contains(modelId.get())) {
                        // submit to cold start queue
                        entityColdStartQueue.put(origRequest);
                    }
                }
            }

            processCheckpointIteration(0, toProcess, successfulRequests, retryableRequests);
        }, exception -> {
            if (ExceptionUtil.isOverloaded(exception)) {
                LOG.error("too many get AD model checkpoint requests or shard not avialble");
                setCoolDownStart();
            } else if (ExceptionUtil.isRetryAble(exception)) {
                // retry all of them
                super.putAll(toProcess);
            } else {
                LOG.error("Fail to restore models", exception);
            }
        });
    }

    private void processCheckpointIteration(
        int i,
        List<EntityFeatureRequest> toProcess,
        Map<String, MultiGetItemResponse> successfulRequests,
        Set<String> retryableRequests
    ) {
        if (i >= toProcess.size()) {
            return;
        }

        // whether we will process next response in callbacks
        // if false, finally will process next checkpoints
        boolean processNextInCallBack = false;
        try {
            EntityFeatureRequest origRequest = toProcess.get(i);

            Optional<String> modelIdOptional = origRequest.getModelId();
            if (false == modelIdOptional.isPresent()) {
                return;
            }

            String detectorId = origRequest.getDetectorId();
            Entity entity = origRequest.getEntity();

            String modelId = modelIdOptional.get();

            MultiGetItemResponse checkpointResponse = successfulRequests.get(modelId);

            if (checkpointResponse != null) {
                // successful requests
                Optional<Map<String, Object>> checkpointString = checkpointDao.processRawCheckpoint(checkpointResponse.getResponse());
                if (checkpointString.isPresent()) {
                    // checkpoint exists
                    Optional<Entry<EntityModel, Instant>> checkpoint = checkpointDao
                        .fromEntityModelCheckpoint(checkpointString.get(), modelId);

                    if (false == checkpoint.isPresent()) {
                        // checkpoint is too big
                        return;
                    }

                    ModelState<EntityModel> modelState = new ModelState<>(
                        new EntityModel(entity, new ArrayDeque<>(), null, null),
                        modelId,
                        detectorId,
                        ModelType.ENTITY.getName(),
                        clock,
                        0
                    );

                    modelState = modelManager.processEntityCheckpoint(checkpoint, modelState);

                    EntityModel entityModel = modelState.getModel();

                    if (entityModel.getRcf() == null || entityModel.getThreshold() == null) {
                        entityColdStarter.trainModelFromExistingSamples(modelState);
                    }

                    ThresholdingResult result = null;
                    if (entityModel.getRcf() != null && entityModel.getThreshold() != null) {
                        result = modelManager.score(origRequest.getCurrentFeature(), modelId, modelState);
                    } else {
                        entityModel.addSample(origRequest.getCurrentFeature());
                    }

                    nodeStateManager
                        .getAnomalyDetector(
                            detectorId,
                            onGetDetector(origRequest, i, detectorId, result, toProcess, successfulRequests, retryableRequests, modelState)
                        );
                    processNextInCallBack = true;
                } else {
                    // checkpoint does not exist
                    // submit to cold start queue
                    entityColdStartQueue.put(origRequest);
                }
            } else if (retryableRequests != null && retryableRequests.contains(modelId)) {
                // failed requests
                super.put(origRequest);
            }
        } finally {
            if (false == processNextInCallBack) {
                processCheckpointIteration(i + 1, toProcess, successfulRequests, retryableRequests);
            }
        }
    }

    private ActionListener<Optional<AnomalyDetector>> onGetDetector(
        EntityFeatureRequest origRequest,
        int index,
        String detectorId,
        ThresholdingResult result,
        List<EntityFeatureRequest> toProcess,
        Map<String, MultiGetItemResponse> successfulRequests,
        Set<String> retryableRequests,
        ModelState<EntityModel> modelState
    ) {
        return ActionListener.wrap(detectorOptional -> {
            if (false == detectorOptional.isPresent()) {
                LOG.warn(new ParameterizedMessage("AnomalyDetector [{}] is not available.", detectorId));
                processCheckpointIteration(index + 1, toProcess, successfulRequests, retryableRequests);
                return;
            }

            AnomalyDetector detector = detectorOptional.get();

            if (result != null && result.getRcfScore() > 0) {
                resultWriteQueue
                    .put(
                        new ResultWriteRequest(
                            origRequest.getExpirationEpochMs(),
                            detectorId,
                            result.getGrade() > 0 ? SegmentPriority.HIGH : SegmentPriority.MEDIUM,
                            new AnomalyResult(
                                detectorId,
                                result.getRcfScore(),
                                result.getGrade(),
                                result.getConfidence(),
                                ParseUtils.getFeatureData(origRequest.getCurrentFeature(), detector),
                                Instant.ofEpochMilli(origRequest.getDataStartTimeMillis()),
                                Instant.ofEpochMilli(origRequest.getDataStartTimeMillis() + detector.getDetectorIntervalInMilliseconds()),
                                Instant.now(),
                                Instant.now(),
                                null,
                                origRequest.getEntity(),
                                detector.getUser(),
                                indexUtil.getSchemaVersion(ADIndex.RESULT)
                            )
                        )
                    );
            }

            // try to load to cache
            boolean loaded = cacheProvider.get().hostIfPossible(detector, modelState);

            if (false == loaded) {
                // not in memory. Maybe cold entities or some other entities
                // have filled the slot while waiting for loading checkpoints.
                checkpointWriteQueue.write(modelState, true, SegmentPriority.LOW);
            }

            processCheckpointIteration(index + 1, toProcess, successfulRequests, retryableRequests);
        }, exception -> {
            LOG.error(new ParameterizedMessage("fail to get checkpoint [{}]", modelState.getModelId()), exception);
            nodeStateManager.setException(detectorId, exception);
            processCheckpointIteration(index + 1, toProcess, successfulRequests, retryableRequests);
        });
    }
}
