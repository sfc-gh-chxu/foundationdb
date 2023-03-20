/*
 * BlobGranulePolicyFlush.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// #include "fdbserver/BlobWorker.h"
#include "fdbserver/BlobWorkerFlushPolicyEngine.actor.h"
#include "flow/Trace.h"

// #include "flow/FastRef.h"
// #include "flow/actorcompiler.h" // has to be last include

void PolicyEngine::addBufferedBytes(int64_t bufferedMutationBytes, BlobWorkerStats* stats) {
	this->globleMutationBytesBuffered += bufferedMutationBytes;
	stats->mutationBytesBuffered += bufferedMutationBytes;

	if (stats->mutationBytesBuffered >= this->fullMemoryProvision) {
		TraceEvent("XCH:BlobWorkerBufferedBytes")
		    .detail("BufferedMutationBytes", this->globleMutationBytesBuffered)
		    .detail("StatsBufferedMutationBytes", stats->mutationBytesBuffered);
	}
}

void PolicyEngine::removeBufferedBytes(int64_t bufferedMutationBytes, BlobWorkerStats* stats) {
	this->globleMutationBytesBuffered -= bufferedMutationBytes;
	TraceEvent("XCH:RemovedBufferedBytes").detail("BufferedMutationBytes", this->globleMutationBytesBuffered);
	stats->mutationBytesBuffered -= bufferedMutationBytes;
}

bool PolicyEngine::checkTooBigDeltaFile(int64_t bufferedDeltaBytes,
                                        int64_t writeAmpDeltaBytes,
                                        int64_t bytesBeforeCompact) {
	if (flushPolicy == singleGranuleFlush) {
		return bufferedDeltaBytes >= writeAmpDeltaBytes;
	} else if (flushPolicy == topKMemoryGranuleFlush) {
		return bufferedDeltaBytes >= bytesBeforeCompact;
	} else {
		return false;
	}
}