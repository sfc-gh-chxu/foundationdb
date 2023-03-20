/*
 * BlobWorkerFlushPolicyEngine.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_BLOBWORKER_FLUSH_POLICY_ENGINE_G_H)
#define FDBSERVER_BLOBWORKER_FLUSH_POLICY_ENGINE_G_H
#include "fdbserver/BlobWorkerFlushPolicyEngine.actor.g.h"
#elif !defined(FDBSERVER_BLOBWORKER_FLUSH_POLICY_ENGINE_ACTOR_H)
#define FDBSERVER_BLOBWORKER_FLUSH_POLICY_ENGINE_ACTOR_H

#pragma once

#include "fdbserver/BlobGranuleServerCommon.actor.h"
#include "fdbclient/BlobWorkerCommon.h"

#include "flow/FastRef.h"
#include "flow/actorcompiler.h" // has to be last include
//
// This module offers different policies to write/flush the delta file. It can also monitor the whole memory usage
//

struct PolicyEngine {
	// belong to bwData;
	// PromiseStream<Future<Void>> addActor;
	enum FlushPolicy { singleGranuleFlush = 0, topKMemoryGranuleFlush = 1, topMemoryGranuleFlush = 2, END };

	PolicyEngine() : globleMutationBytesBuffered(0) {
		flushPolicy = (FlushPolicy)(SERVER_KNOBS->BLOB_WORKER_FLUSH_POLICY);
		if (flushPolicy == topKMemoryGranuleFlush) {
			fullMemoryProvision = SERVER_KNOBS->BLOB_WORKER_MEMORY_PROVISION;
		}
	}

	void addBufferedBytes(int64_t bufferedMutationBytes, BlobWorkerStats* stats);
	void removeBufferedBytes(int64_t bufferedMutationBytes, BlobWorkerStats* stats);
	bool checkTooBigDeltaFile(int64_t bufferedDeltaBytes, int64_t writeAmpDeltaBytes, int64_t bytesBeforeCompact);

private:
	uint32_t flushPolicy;
	int64_t globleMutationBytesBuffered;
	int64_t fullMemoryProvision;
};

#endif