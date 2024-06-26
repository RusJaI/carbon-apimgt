/*
 * Copyright (c) 2024, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.apimgt.throttling.siddhi.extension;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.AbstractMap;
import java.util.Map;

/**
 * This is a custom extension, written to add reset functionality to existing count function.
 * Upon arrival of a reset request, if the second argument of the count function is true,
 * the counter related to the specific throttle key will be reset to zero.
 * <p/>
 * Usage:
 * throttler:count(messageID, true)
 * <p/>
 * Example on usage:
 * FROM EligibilityStream[isEligible==true]#throttler:timeBatch(1 hour, 0)
 * select throttleKey, throttler:count(messageID,cast(map:get(propertiesMap,'reset'),'bool'))>= 5 as isThrottled,
 * expiryTimeStamp group by throttleKey
 * INSERT ALL EVENTS into ResultStream;
 */
public class CountAttributeAggregatorWithReset extends AttributeAggregator {
    private static Attribute.Type type = Attribute.Type.LONG;
    private long value = 0l;

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param executionPlanContext         Execution plan runtime context
     */
    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {

    }

    public Attribute.Type getReturnType() {
        return type;
    }

    @Override
    public Object processAdd(Object data) {
        value++;
        return value;
    }

    @Override
    public Object processAdd(Object[] data) {
        //reset the counter to zero if the second parameter is true
        if (Boolean.TRUE.equals(data[1])){
            return reset();
        }
        value++;
        return value;
    }

    @Override
    public Object processRemove(Object data) {
        value--;
        return value;
    }

    @Override
    public Object processRemove(Object[] data) {
        value--;
        return value;
    }

    @Override
    public Object reset() {
        value = 0l;
        return value;
    }

    @Override
    public void start() {
        //Nothing to start
    }

    @Override
    public void stop() {
        //nothing to stop
    }

    @Override
    public Object[] currentState() {
        return new Object[]{new AbstractMap.SimpleEntry<String, Object>("Value", value)};
    }

    @Override
    public void restoreState(Object[] state) {
        Map.Entry<String, Object> stateEntry = (Map.Entry<String, Object>) state[0];
        value = (Long) stateEntry.getValue();
    }

}
