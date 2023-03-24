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

#include "fdbserver/BlobWorkerFlushPolicyEngine.actor.h"
#include "fdbserver/BlobWorker.h"
#include "flow/Trace.h"

// #include "flow/FastRef.h"
// #include "flow/actorcompiler.h" // has to be last include

void BlobWorkerFlushPolicyEngine::addBufferedBytes(int64_t bufferedMutationBytes, BlobWorkerStats* stats) {
	this->globleMutationBytesBuffered += bufferedMutationBytes;
	stats->mutationBytesBuffered += bufferedMutationBytes;

	if (stats->mutationBytesBuffered >= this->fullMemoryProvision) {
		TraceEvent("XCH:BlobWorkerBufferedBytes")
		    .detail("BufferedMutationBytes", this->globleMutationBytesBuffered)
		    .detail("StatsBufferedMutationBytes", stats->mutationBytesBuffered);

		if (memoryFull.canBeSet()) {
			memoryFull.send(Void());
		}
	}
}

void BlobWorkerFlushPolicyEngine::removeBufferedBytes(int64_t bufferedMutationBytes, BlobWorkerStats* stats) {
	this->globleMutationBytesBuffered -= bufferedMutationBytes;
	TraceEvent("XCH:RemovedBufferedBytes").detail("BufferedMutationBytes", this->globleMutationBytesBuffered);
	stats->mutationBytesBuffered -= bufferedMutationBytes;
}

bool BlobWorkerFlushPolicyEngine::checkTooBigDeltaFile(int64_t bufferedDeltaBytes,
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

void BlobWorkerFlushPolicyEngine::start(Reference<BlobWorkerData> bwData) {
	if (flushPolicy == singleGranuleFlush) {
		return;
	} else if (flushPolicy == topKMemoryGranuleFlush) {
		bwData->bwPolicyEngine->monitorFuture = this->monitorAndflushTopKMemory(bwData);
	}
	return;
}

ACTOR Future<Void> BlobWorkerFlushPolicyEngine::monitorAndflushTopKMemory(Reference<BlobWorkerData> bwData) {
	loop {
		wait(bwData->bwPolicyEngine->memoryFull.getFuture());
		TraceEvent("XCH::EnterFunctionFlushTopKMemory");

		auto allRanges = bwData->granuleMetadata.intersectingRanges(normalKeys);
		std::vector<GranuleMemoryBufferedEntry> granuleMetadataEntrys;
		for (auto& it : allRanges) {
			if (it.value().activeMetadata.isValid() && it.value().activeMetadata->cancelled.canBeSet()) {
				auto metadata = it.value().activeMetadata;
				granuleMetadataEntrys.push_back(GranuleMemoryBufferedEntry(metadata->bufferedDeltaBytes, metadata));
			}
		}
		sort(granuleMetadataEntrys.begin(), granuleMetadataEntrys.end(), OrderForTopKMemory());

		for (auto& it : granuleMetadataEntrys) {
			TraceEvent(SevDebug, "XCH::BlobGranuleDeltaFile", bwData->id)
			    .detail("BlobGranuleDeltaFile", it.memoryBuffered)
				.detail("Granule", it.granule->keyRange);
		} 

		std::vector<Future<Void>> futures;
		int topK = bwData->bwPolicyEngine->topK;
		int topNonEmptyGranule = std::min(topK, int(granuleMetadataEntrys.size()));
		for (int i = 0; i < topK; i++) {
			if (granuleMetadataEntrys[i].memoryBuffered == 0) {
				topNonEmptyGranule = i;
				break;
			}
		}
		ASSERT(topNonEmptyGranule > 0);
		futures.reserve(topNonEmptyGranule);
		for (int i = 0; i < topNonEmptyGranule; i++) {
			TraceEvent("XCH::Flag").detail("GranuleInformation",granuleMetadataEntrys[i].granule->keyRange);
			granuleMetadataEntrys[i].granule->topMemoryFlush.trigger();
			granuleMetadataEntrys[i].granule->topMemoryFlushTest = true;

			Future<Void> waitForTopMemoryFlushComplete =
			    granuleMetadataEntrys[i].granule->durableDeltaVersion.whenAtLeast(
			        granuleMetadataEntrys[i].granule->bufferedDeltaVersion);
			futures.push_back(waitForTopMemoryFlushComplete);
		}
		// ASSERT(futures.size() > 0);
		TraceEvent("XCH::Flag1");
		wait(waitForAll(futures));
		TraceEvent("XCH::FinishedFunctionFlushTopKMemory");

		bwData->bwPolicyEngine->memoryFull.reset();
	}
}