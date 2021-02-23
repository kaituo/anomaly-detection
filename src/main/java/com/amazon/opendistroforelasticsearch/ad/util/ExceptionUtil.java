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

package com.amazon.opendistroforelasticsearch.ad.util;

import java.util.EnumSet;
import java.util.concurrent.RejectedExecutionException;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.core.util.Throwables;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.RestStatus;

import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.EndRunException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ResourceNotFoundException;

public class ExceptionUtil {
    public static final String RESOURCE_NOT_FOUND_EXCEPTION_NAME_UNDERSCORE = ElasticsearchException
        .getExceptionName(new ResourceNotFoundException("", ""));

    // a positive cache of retriable error rest status
    private static final EnumSet<RestStatus> RETRYABLE_STATUS = EnumSet
        .of(RestStatus.REQUEST_TIMEOUT, RestStatus.CONFLICT, RestStatus.INTERNAL_SERVER_ERROR);

    /**
     * Elasticsearch restricts the kind of exceptions can be thrown over the wire
     * (See ElasticsearchException.ElasticsearchExceptionHandle). Since we cannot
     * add our own exception like ResourceNotFoundException without modifying
     * Elasticsearch's code, we have to unwrap the remote transport exception and
     * check its root cause message.
     *
     * @param exception exception thrown locally or over the wire
     * @param expected  expected root cause
     * @param expectedExceptionName expected exception name
     * @return whether the exception wraps the expected exception as the cause
     */
    public static boolean isException(Throwable exception, Class<? extends Exception> expected, String expectedExceptionName) {
        if (exception == null) {
            return false;
        }

        if (expected.isAssignableFrom(exception.getClass())) {
            return true;
        }

        // all exception that has not been registered to sent over wire can be wrapped
        // inside NotSerializableExceptionWrapper.
        // see StreamOutput.writeException
        // ElasticsearchException.getExceptionName(exception) returns exception
        // separated by underscore. For example, ResourceNotFoundException is converted
        // to "resource_not_found_exception".
        if (exception instanceof NotSerializableExceptionWrapper && exception.getMessage().trim().startsWith(expectedExceptionName)) {
            return true;
        }
        return false;
    }

    /**
     * Get failure of all shards.
     *
     * @param response index response
     * @return composite failures of all shards
     */
    public static String getShardsFailure(IndexResponse response) {
        StringBuilder failureReasons = new StringBuilder();
        if (response.getShardInfo() != null && response.getShardInfo().getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure : response.getShardInfo().getFailures()) {
                failureReasons.append(failure.reason());
            }
            return failureReasons.toString();
        }
        return null;
    }

    /**
     * Count exception in AD failure stats of not.
     *
     * @param e exception
     * @return true if should count in AD failure stats; otherwise return false
     */
    public static boolean countInStats(Exception e) {
        if (!(e instanceof AnomalyDetectionException) || ((AnomalyDetectionException) e).isCountedInStats()) {
            return true;
        }
        return false;
    }

    /**
     * Get error message from exception.
     *
     * @param e exception
     * @return readable error message or full stack trace
     */
    public static String getErrorMessage(Exception e) {
        if (e instanceof IllegalArgumentException || e instanceof AnomalyDetectionException) {
            return e.getMessage();
        } else if (e instanceof ElasticsearchException) {
            return ((ElasticsearchException) e).getDetailedMessage();
        } else {
            return ExceptionUtils.getFullStackTrace(e);
        }
    }

    /**
     *
     * @param exception Exception
     * @return whether the cause indicates the cluster is overloaded
     */
    public static boolean isOverloaded(Throwable exception) {
        Throwable cause = Throwables.getRootCause(exception);
        // LimitExceededException may indicate circuit breaker exception
        return cause instanceof RejectedExecutionException
            || TransportActions.isShardNotAvailableException(cause)
            || cause instanceof LimitExceededException;
    }

    public static boolean isRetryAble(Exception e) {
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        RestStatus status = ExceptionsHelper.status(cause);
        return RETRYABLE_STATUS.contains(status);
    }

    public static boolean isRetryAble(RestStatus status) {
        return RETRYABLE_STATUS.contains(status);
    }

    public static boolean isNotFound(Exception e) {
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        RestStatus status = ExceptionsHelper.status(cause);
        return status == RestStatus.NOT_FOUND;
    }

    /**
     * Wrap a listener to return the given exception no matter what
     * @param <T> The type of listener response
     * @param original Original listener
     * @param exceptionToReturn The exception to return
     * @param detectorId Detector Id
     * @return the wrapped listener
     */
    public static <T> ActionListener<T> wrapListener(ActionListener<T> original, Exception exceptionToReturn, String detectorId) {
        return ActionListener
            .wrap(
                r -> { original.onFailure(exceptionToReturn); },
                e -> { original.onFailure(selectHigherPriorityException(exceptionToReturn, e)); }
            );
    }

    /**
     * Return an exception that has higher priority.
     * If an exception is EndRunException while another one is not, the former has
     *  higher priority.
     * If both exceptions are EndRunException, the one with end now true has higher
     *  priority.
     * Otherwise, return the second given exception.
     * @param exception1 Exception 1
     * @param exception2 Exception 2
     * @return high priority exception
     */
    public static Exception selectHigherPriorityException(Exception exception1, Exception exception2) {
        if (exception1 instanceof EndRunException) {
            // we have already had EndRunException. Don't replace it with something less severe
            EndRunException endRunException = (EndRunException) exception1;
            if (endRunException.isEndNow()) {
                // don't proceed if recorded exception is ending now
                return exception1;
            }
            if (false == (exception2 instanceof EndRunException) || false == ((EndRunException) exception2).isEndNow()) {
                // don't proceed if the giving exception is not ending now
                return exception1;
            }
        }
        return exception2;
    }

    public static boolean isIndexNotAvailable(Exception e) {
        if (e == null) {
            return false;
        }
        return e instanceof IndexNotFoundException || e instanceof NoShardAvailableActionException;
    }
}
