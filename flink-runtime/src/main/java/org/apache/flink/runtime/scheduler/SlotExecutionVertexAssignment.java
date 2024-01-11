/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
/**
 * @mark: ExecutionVertex与分配的slot的对应关系
 * 申请slot过程中有两种关于slot的抽象：
 * 1. LogicalSlot 逻辑slot
 * 2. PhysicalSlot 物理slot
 * 共享slot的机制，可能存在多个Task共用slot的情况
 * 每个去申请slot的Task，都会申请到一个LogicalSlot，有可能多个任务申请到的LogicalSlot是同一个PhysicalSlot
 */
/** The slot assignment for a {@link ExecutionVertex}. */
class SlotExecutionVertexAssignment {

    private final ExecutionVertexID executionVertexId;

    private final CompletableFuture<LogicalSlot> logicalSlotFuture;

    SlotExecutionVertexAssignment(
            ExecutionVertexID executionVertexId, CompletableFuture<LogicalSlot> logicalSlotFuture) {
        this.executionVertexId = checkNotNull(executionVertexId);
        this.logicalSlotFuture = checkNotNull(logicalSlotFuture);
    }

    ExecutionVertexID getExecutionVertexId() {
        return executionVertexId;
    }

    CompletableFuture<LogicalSlot> getLogicalSlotFuture() {
        return logicalSlotFuture;
    }
}
