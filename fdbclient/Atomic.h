/*
 * Atomic.h
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

#ifndef FLOW_FDBCLIENT_ATOMIC_H
#define FLOW_FDBCLIENT_ATOMIC_H
#pragma once

#include "fdbclient/CommitTransaction.h"

static ValueRef doLittleEndianAdd(const Optional<ValueRef>& existingValueOptional,
                                  const ValueRef& otherOperand,
                                  Arena& ar) {
	const ValueRef& existingValue = existingValueOptional.present() ? existingValueOptional.get() : StringRef();
	if (!existingValue.size())
		return otherOperand;
	if (!otherOperand.size())
		return otherOperand;

	uint8_t* buf = new (ar) uint8_t[otherOperand.size()];
	int i = 0;
	int carry = 0;

	for (i = 0; i < std::min(existingValue.size(), otherOperand.size()); i++) {
		int sum = existingValue[i] + otherOperand[i] + carry;
		buf[i] = sum;
		carry = sum >> 8;
	}
	for (; i < otherOperand.size(); i++) {
		int sum = otherOperand[i] + carry;
		buf[i] = sum;
		carry = sum >> 8;
	}

	return StringRef(buf, i);
}

static ValueRef doAnd(const Optional<ValueRef>& existingValueOptional, const ValueRef& otherOperand, Arena& ar) {
	const ValueRef& existingValue = existingValueOptional.present() ? existingValueOptional.get() : StringRef();
	if (!otherOperand.size())
		return otherOperand;

	uint8_t* buf = new (ar) uint8_t[otherOperand.size()];
	int i = 0;

	for (i = 0; i < std::min(existingValue.size(), otherOperand.size()); i++)
		buf[i] = existingValue[i] & otherOperand[i];
	for (; i < otherOperand.size(); i++)
		buf[i] = 0x0;

	return StringRef(buf, i);
}

static ValueRef doAndV2(const Optional<ValueRef>& existingValueOptional, const ValueRef& otherOperand, Arena& ar) {
	if (!existingValueOptional.present())
		return otherOperand;

	return doAnd(existingValueOptional, otherOperand, ar);
}

static ValueRef doOr(const Optional<ValueRef>& existingValueOptional, const ValueRef& otherOperand, Arena& ar) {
	const ValueRef& existingValue = existingValueOptional.present() ? existingValueOptional.get() : StringRef();
	if (!existingValue.size())
		return otherOperand;
	if (!otherOperand.size())
		return otherOperand;

	uint8_t* buf = new (ar) uint8_t[otherOperand.size()];
	int i = 0;

	for (i = 0; i < std::min(existingValue.size(), otherOperand.size()); i++)
		buf[i] = existingValue[i] | otherOperand[i];
	for (; i < otherOperand.size(); i++)
		buf[i] = otherOperand[i];

	return StringRef(buf, i);
}

static ValueRef doXor(const Optional<ValueRef>& existingValueOptional, const ValueRef& otherOperand, Arena& ar) {
	const ValueRef& existingValue = existingValueOptional.present() ? existingValueOptional.get() : StringRef();
	if (!existingValue.size())
		return otherOperand;
	if (!otherOperand.size())
		return otherOperand;

	uint8_t* buf = new (ar) uint8_t[otherOperand.size()];
	int i = 0;

	for (i = 0; i < std::min(existingValue.size(), otherOperand.size()); i++)
		buf[i] = existingValue[i] ^ otherOperand[i];

	for (; i < otherOperand.size(); i++)
		buf[i] = otherOperand[i];

	return StringRef(buf, i);
}

static ValueRef doAppendIfFits(const Optional<ValueRef>& existingValueOptional,
                               const ValueRef& otherOperand,
                               Arena& ar) {
	const ValueRef& existingValue = existingValueOptional.present() ? existingValueOptional.get() : StringRef();
	if (!existingValue.size())
		return otherOperand;
	if (!otherOperand.size())
		return existingValue;
	if (existingValue.size() + otherOperand.size() > CLIENT_KNOBS->VALUE_SIZE_LIMIT) {
		TEST(true) // AppendIfFIts resulted in truncation
		return existingValue;
	}

	uint8_t* buf = new (ar) uint8_t[existingValue.size() + otherOperand.size()];
	int i, j;

	for (i = 0; i < existingValue.size(); i++)
		buf[i] = existingValue[i];

	for (j = 0; j < otherOperand.size(); j++)
		buf[i + j] = otherOperand[j];

	return StringRef(buf, i + j);
}

static ValueRef doMax(const Optional<ValueRef>& existingValueOptional, const ValueRef& otherOperand, Arena& ar) {
	const ValueRef& existingValue = existingValueOptional.present() ? existingValueOptional.get() : StringRef();
	if (!existingValue.size())
		return otherOperand;
	if (!otherOperand.size())
		return otherOperand;

	int i, j;

	for (i = otherOperand.size() - 1; i >= existingValue.size(); i--) {
		if (otherOperand[i] != 0) {
			return otherOperand;
		}
	}

	for (; i >= 0; i--) {
		if (otherOperand[i] > existingValue[i]) {
			return otherOperand;
		} else if (otherOperand[i] < existingValue[i]) {
			uint8_t* buf = new (ar) uint8_t[otherOperand.size()];
			for (j = 0; j < std::min(existingValue.size(), otherOperand.size()); j++) {
				buf[j] = existingValue[j];
			}
			for (; j < otherOperand.size(); j++) {
				buf[j] = 0x0;
			}
			return StringRef(buf, j);
		}
	}

	return otherOperand;
}

static ValueRef doByteMax(const Optional<ValueRef>& existingValueOptional, const ValueRef& otherOperand, Arena& ar) {
	if (!existingValueOptional.present())
		return otherOperand;

	const ValueRef& existingValue = existingValueOptional.get();
	if (existingValue > otherOperand)
		return existingValue;

	return otherOperand;
}

static ValueRef doMin(const Optional<ValueRef>& existingValueOptional, const ValueRef& otherOperand, Arena& ar) {
	if (!otherOperand.size())
		return otherOperand;

	const ValueRef& existingValue = existingValueOptional.present() ? existingValueOptional.get() : StringRef();
	int i, j;

	for (i = otherOperand.size() - 1; i >= existingValue.size(); i--) {
		if (otherOperand[i] != 0) {
			uint8_t* buf = new (ar) uint8_t[otherOperand.size()];
			for (j = 0; j < std::min(existingValue.size(), otherOperand.size()); j++) {
				buf[j] = existingValue[j];
			}
			for (; j < otherOperand.size(); j++) {
				buf[j] = 0x0;
			}
			return StringRef(buf, j);
		}
	}

	for (; i >= 0; i--) {
		if (otherOperand[i] > existingValue[i]) {
			uint8_t* buf = new (ar) uint8_t[otherOperand.size()];
			for (j = 0; j < std::min(existingValue.size(), otherOperand.size()); j++) {
				buf[j] = existingValue[j];
			}
			for (; j < otherOperand.size(); j++) {
				buf[j] = 0x0;
			}
			return StringRef(buf, j);
		} else if (otherOperand[i] < existingValue[i]) {
			return otherOperand;
		}
	}

	return otherOperand;
}

static ValueRef doMinV2(const Optional<ValueRef>& existingValueOptional, const ValueRef& otherOperand, Arena& ar) {
	if (!existingValueOptional.present())
		return otherOperand;

	return doMin(existingValueOptional, otherOperand, ar);
}

static ValueRef doByteMin(const Optional<ValueRef>& existingValueOptional, const ValueRef& otherOperand, Arena& ar) {
	if (!existingValueOptional.present())
		return otherOperand;

	const ValueRef& existingValue = existingValueOptional.get();
	if (existingValue < otherOperand)
		return existingValue;

	return otherOperand;
}

static Optional<ValueRef> doCompareAndClear(const Optional<ValueRef>& existingValueOptional,
                                            const ValueRef& otherOperand,
                                            Arena& ar) {
	if (!existingValueOptional.present() || existingValueOptional.get() == otherOperand) {
		// Clear the value.
		return Optional<ValueRef>();
	}
	return existingValueOptional; // No change required.
}

/*
 * Returns the range corresponding to the specified versionstamp key.
 */
static KeyRangeRef getVersionstampKeyRange(Arena& arena, const KeyRef& key, const KeyRef& maxKey) {
	KeyRef begin(arena, key);
	KeyRef end(arena, key);

	if (begin.size() < 4)
		throw client_invalid_operation();

	int32_t pos;
	memcpy(&pos, begin.end() - sizeof(int32_t), sizeof(int32_t));
	pos = littleEndian32(pos);
	begin = begin.substr(0, begin.size() - 4);
	end = end.substr(0, end.size() - 3);
	mutateString(end)[end.size() - 1] = 0;

	if (pos < 0 || pos + 10 > begin.size())
		throw client_invalid_operation();

	memset(mutateString(begin) + pos, 0, 10);
	memset(mutateString(end) + pos, '\xff', 10);

	return KeyRangeRef(begin, std::min(end, maxKey));
}

static void placeVersionstamp(uint8_t* destination, Version version, uint16_t transactionNumber) {
	version = bigEndian64(version);
	transactionNumber = bigEndian16(transactionNumber);
	static_assert(sizeof(version) == 8, "version size mismatch");
	memcpy(destination, &version, sizeof(version));
	static_assert(sizeof(transactionNumber) == 2, "txn num size mismatch");
	memcpy(destination + sizeof(version), &transactionNumber, sizeof(transactionNumber));
}

static void transformVersionstampMutation(MutationRef& mutation,
                                          StringRef MutationRef::*param,
                                          Version version,
                                          uint16_t transactionNumber) {
	if ((mutation.*param).size() >= 4) {
		int32_t pos;
		memcpy(&pos, (mutation.*param).end() - sizeof(int32_t), sizeof(int32_t));
		pos = littleEndian32(pos);
		mutation.*param = (mutation.*param).substr(0, (mutation.*param).size() - 4);

		if (pos >= 0 && pos + 10 <= (mutation.*param).size()) {
			placeVersionstamp(mutateString(mutation.*param) + pos, version, transactionNumber);
		}
	}

	mutation.type = MutationRef::SetValue;
}

#endif
