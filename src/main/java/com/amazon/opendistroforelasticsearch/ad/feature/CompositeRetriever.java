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

package com.amazon.opendistroforelasticsearch.ad.feature;

import java.io.IOException;
import java.time.Clock;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.amazon.opendistroforelasticsearch.ad.common.exception.EndRunException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.Entity;
import com.amazon.opendistroforelasticsearch.ad.model.Feature;
import com.amazon.opendistroforelasticsearch.ad.util.ParseUtils;

public class CompositeRetriever extends AbstractRetriever {
    private static final String AGG_NAME_COMP = "comp_agg";
    private static final Logger LOG = LogManager.getLogger(CompositeRetriever.class);

    private final long dataStartEpoch;
    private final long dataEndEpoch;
    private final AnomalyDetector anomalyDetector;
    private final NamedXContentRegistry xContent;
    private final Client client;
    private int totalResults;
    private int maxEntities;
    private final int pageSize;
    private long expirationEpochMs;
    private Clock clock;

    public CompositeRetriever(
        long dataStartEpoch,
        long dataEndEpoch,
        AnomalyDetector anomalyDetector,
        NamedXContentRegistry xContent,
        Client client,
        long expirationEpochMs,
        Clock clock,
        Settings settings,
        int maxEntitiesPerInterval,
        int pageSize
    ) {
        this.dataStartEpoch = dataStartEpoch;
        this.dataEndEpoch = dataEndEpoch;
        this.anomalyDetector = anomalyDetector;
        this.xContent = xContent;
        this.client = client;
        this.totalResults = 0;
        this.maxEntities = maxEntitiesPerInterval;
        this.pageSize = pageSize;
        this.expirationEpochMs = expirationEpochMs;
        this.clock = clock;
    }

    // a constructor that provide default value of clock
    public CompositeRetriever(
        long dataStartEpoch,
        long dataEndEpoch,
        AnomalyDetector anomalyDetector,
        NamedXContentRegistry xContent,
        Client client,
        long expirationEpochMs,
        Settings settings,
        int maxEntitiesPerInterval,
        int pageSize
    ) {
        this(
            dataStartEpoch,
            dataEndEpoch,
            anomalyDetector,
            xContent,
            client,
            expirationEpochMs,
            Clock.systemUTC(),
            settings,
            maxEntitiesPerInterval,
            pageSize
        );
    }

    public void start(ActionListener<Page> listener) {
        try {
            RangeQueryBuilder rangeQuery = new RangeQueryBuilder(anomalyDetector.getTimeField())
                .gte(dataStartEpoch)
                .lt(dataEndEpoch)
                .format("epoch_millis");

            BoolQueryBuilder internalFilterQuery = new BoolQueryBuilder().filter(anomalyDetector.getFilterQuery()).filter(rangeQuery);

            // multiple categorical fields are supported
            CompositeAggregationBuilder composite = AggregationBuilders
                .composite(
                    AGG_NAME_COMP,
                    anomalyDetector
                        .getCategoryField()
                        .stream()
                        .map(f -> new TermsValuesSourceBuilder(f).field(f))
                        .collect(Collectors.toList())
                )
                .size(pageSize);
            for (Feature feature : anomalyDetector.getFeatureAttributes()) {
                AggregatorFactories.Builder internalAgg = ParseUtils
                    .parseAggregators(feature.getAggregation().toString(), xContent, feature.getId());
                composite.subAggregation(internalAgg.getAggregatorFactories().iterator().next());
            }

            // In order to optimize the early termination it is advised to set track_total_hits in the request to false.
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(internalFilterQuery)
                .size(0)
                .aggregation(composite)
                .trackTotalHits(false);

            Page page = new Page(null, searchSourceBuilder, null);
            page.next(client, anomalyDetector, listener);

        } catch (Exception e) {
            listener
                .onFailure(new EndRunException(anomalyDetector.getDetectorId(), CommonErrorMessages.INVALID_SEARCH_QUERY_MSG, e, false));
        }
    }

    public class Page {
        // a map from categorical field name to values (type: java.lang.Comparable)
        Map<String, Object> afterKey;
        SearchSourceBuilder source;
        Map<Entity, double[]> results;

        public Page(Map<String, Object> afterKey, SearchSourceBuilder source, Map<Entity, double[]> results) {
            this.afterKey = afterKey;
            this.source = source;
            this.results = results;
        }

        public void next(Client client, AnomalyDetector anomalyDetector, ActionListener<Page> listener) {
            SearchRequest searchRequest = new SearchRequest(anomalyDetector.getIndices().toArray(new String[0]), source);
            client.search(searchRequest, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse response) {
                    processResponse(response, () -> client.search(searchRequest, this), listener);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }

        private void processResponse(SearchResponse response, Runnable retry, ActionListener<Page> listener) {
            if (shouldRetryDueToEmptyPage(response)) {
                updateCompositeAfterKey(response, source);
                retry.run();
                return;
            }

            try {
                Page page = analyzePage(response);
                if (totalResults < maxEntities && page.afterKey != null) {
                    updateCompositeAfterKey(response, source);
                    listener.onResponse(page);
                } else {
                    listener.onResponse(null);
                }
            } catch (Exception ex) {
                listener.onFailure(ex);
            }
        }

        /**
         *
         * @param response current response
         * @return A page containing
         *  ** the after key
         *  ** query source builder to next page if any
         *  ** a map of composite keys to its values.  The values are arranged
         *    according to the order of anomalyDetector.getEnabledFeatureIds().
         * @throws IOException when writing to file errs.
         */
        private Page analyzePage(SearchResponse response) throws IOException {
            CompositeAggregation composite = getComposite(response);

            Map<Entity, double[]> results = new HashMap<>();
            /*
             *
             * Example composite aggregation:
             *
             "aggregations": {
                "my_buckets": {
                    "after_key": {
                        "service": "app_6",
                        "host": "server_3"
                    },
                    "buckets": [
                        {
                            "key": {
                                "service": "app_6",
                                "host": "server_3"
                            },
                            "doc_count": 1,
                            "the_max": {
                                "value": -38.0
                            },
                            "the_min": {
                                "value": -38.0
                            }
                        }
                    ]
               }
             }
             */
            for (Bucket bucket : composite.getBuckets()) {
                Optional<double[]> featureValues = parseBucket(bucket, anomalyDetector.getEnabledFeatureIds());
                // bucket.getKey() returns a map of categorical field like "host" and its value like "server_1"
                if (featureValues.isPresent() && bucket.getKey() != null) {
                    results.put(Entity.createEntityByReordering(anomalyDetector.getDetectorId(), bucket.getKey()), featureValues.get());
                }
            }

            totalResults += results.size();

            return new Page(composite.afterKey(), source, results);
        }

        private void updateCompositeAfterKey(SearchResponse r, SearchSourceBuilder search) {
            CompositeAggregation composite = getComposite(r);

            if (composite == null) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "Empty resposne: %s", r));
            }

            updateSourceAfterKey(composite.afterKey(), search);
        }

        private void updateSourceAfterKey(Map<String, Object> afterKey, SearchSourceBuilder search) {
            AggregationBuilder aggBuilder = search.aggregations().getAggregatorFactories().iterator().next();
            // update after-key with the new value
            if (aggBuilder instanceof CompositeAggregationBuilder) {
                CompositeAggregationBuilder comp = (CompositeAggregationBuilder) aggBuilder;
                comp.aggregateAfter(afterKey);
            } else {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Invalid client request; expected a composite builder but instead got {}", aggBuilder)
                );
            }
        }

        private boolean shouldRetryDueToEmptyPage(SearchResponse response) {
            CompositeAggregation composite = getComposite(response);
            // if there are no buckets but a next page, go fetch it instead of sending an empty response to the client
            return composite != null && composite.getBuckets().isEmpty() && composite.afterKey() != null && !composite.afterKey().isEmpty();
        }

        CompositeAggregation getComposite(SearchResponse response) {
            if (response == null || response.getAggregations() == null) {
                return null;
            }
            Aggregation agg = response.getAggregations().get(AGG_NAME_COMP);
            if (agg == null) {
                return null;
            }

            if (agg instanceof CompositeAggregation) {
                return (CompositeAggregation) agg;
            }

            throw new IllegalArgumentException(String.format(Locale.ROOT, "Not a composite response; {}", agg.getClass()));
        }

        public boolean isEmpty() {
            return results == null || results.isEmpty();
        }

        public Map<Entity, double[]> getResults() {
            return results;
        }

        public boolean hasTimeLeft() {
            // next interval has not started
            return expirationEpochMs > clock.millis();
        }

        public void next(ActionListener<Page> listener) {
            this.next(client, anomalyDetector, listener);
        }

        @Override
        public String toString() {
            ToStringBuilder toStringBuilder = new ToStringBuilder(this);

            if (afterKey != null) {
                toStringBuilder.append("afterKey", afterKey);
            }
            if (source != null) {
                toStringBuilder.append("source", source);
            }
            if (results != null) {
                toStringBuilder.append("results", results);
            }

            return toStringBuilder.toString();
        }
    }
}
