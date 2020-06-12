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

package com.amazon.opendistroforelasticsearch.ad.model;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Instant;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import com.amazon.opendistroforelasticsearch.ad.annotation.Generated;
import com.amazon.opendistroforelasticsearch.ad.util.ParseUtils;
import com.google.common.base.Objects;

/**
 * Include anomaly detector's state
 */
public class DetectorInfo implements ToXContentObject, Cloneable {

    public static final String PARSE_FIELD_NAME = "AnomalyInfo";
    public static final NamedXContentRegistry.Entry XCONTENT_REGISTRY = new NamedXContentRegistry.Entry(
        DetectorInfo.class,
        new ParseField(PARSE_FIELD_NAME),
        it -> parse(it)
    );

    public static final String ANOMALY_INFO_INDEX = ".opendistro-anomaly-info";

    public static final String RCF_UPDATES_FIELD = "rcf_updates";
    public static final String LAST_UPDATE_TIME_FIELD = "last_update_time";
    public static final String ERROR_FIELD = "error";

    private long rcfUpdates = -1L;
    private Instant lastUpdateTime = null;
    private String error = null;

    private DetectorInfo() {}

    public static class Builder {
        private long rcfUpdates = -1;
        private Instant lastUpdateTime = null;
        private String error = null;

        public Builder() {}

        public Builder rcfUpdates(long rcfUpdates) {
            this.rcfUpdates = rcfUpdates;
            return this;
        }

        public Builder lastUpdateTime(Instant lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
            return this;
        }

        public Builder error(String error) {
            this.error = error;
            return this;
        }

        public DetectorInfo build() {
            DetectorInfo info = new DetectorInfo();
            info.rcfUpdates = this.rcfUpdates;
            info.lastUpdateTime = this.lastUpdateTime;
            info.error = this.error;

            return info;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();

        if (rcfUpdates >= 0) {
            xContentBuilder.field(RCF_UPDATES_FIELD, rcfUpdates);
        }
        if (lastUpdateTime != null) {
            xContentBuilder.field(LAST_UPDATE_TIME_FIELD, lastUpdateTime.toEpochMilli());
        }
        if (error != null) {
            xContentBuilder.field(ERROR_FIELD, error);
        }
        return xContentBuilder.endObject();
    }

    public static DetectorInfo parse(XContentParser parser) throws IOException {
        long rcfUpdates = -1L;
        Instant lastUpdateTime = null;
        String error = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case RCF_UPDATES_FIELD:
                    rcfUpdates = parser.longValue();
                    break;
                case LAST_UPDATE_TIME_FIELD:
                    lastUpdateTime = ParseUtils.toInstant(parser);
                    break;
                case ERROR_FIELD:
                    error = parser.text();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        return new DetectorInfo.Builder().rcfUpdates(rcfUpdates).lastUpdateTime(lastUpdateTime).error(error).build();
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        DetectorInfo that = (DetectorInfo) o;
        return Objects.equal(getRcfUpdates(), that.getRcfUpdates())
            && Objects.equal(getLastUpdateTime(), that.getLastUpdateTime())
            && Objects.equal(getError(), that.getError());
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects.hashCode(rcfUpdates, lastUpdateTime, error);
    }

    @Override
    public Object clone() {
        DetectorInfo info = null;
        try {
            info = (DetectorInfo) super.clone();
        } catch (CloneNotSupportedException e) {
            info = new DetectorInfo.Builder().rcfUpdates(rcfUpdates).lastUpdateTime(lastUpdateTime).error(error).build();
        }
        return info;
    }

    public long getRcfUpdates() {
        return rcfUpdates;
    }

    public void setRcfUpdates(long rcfUpdates) {
        this.rcfUpdates = rcfUpdates;
    }

    public Instant getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(Instant lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }
}
