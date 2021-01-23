/*
 * StreamCipher.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#ifndef __FLOW_STREAM_CIPHER_H__
#define __FLOW_STREAM_CIPHER_H__

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/flow.h"

#include <openssl/aes.h>
#include <openssl/evp.h>
#include <string>
#include <vector>

class EncryptionStreamCipher final : NonCopyable, public ReferenceCounted<EncryptionStreamCipher> {
	EVP_CIPHER_CTX* ctx;
public:
	EncryptionStreamCipher(unsigned char const* key, unsigned char const* salt);
	~EncryptionStreamCipher();
	StringRef encrypt(unsigned char const* plaintext, int len, Arena&);
	StringRef finish(Arena&);
};

class DecryptionStreamCipher final : NonCopyable, public ReferenceCounted<DecryptionStreamCipher> {
	EVP_CIPHER_CTX* ctx;
public:
	DecryptionStreamCipher(unsigned char const* key, unsigned char const* salt);
	~DecryptionStreamCipher();
	StringRef decrypt(unsigned char const* ciphertext, int len, Arena&);
	StringRef finish(Arena&);
};

#endif
