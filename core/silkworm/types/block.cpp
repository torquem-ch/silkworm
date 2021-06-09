/*
   Copyright 2020-2021 The Silkworm Authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include "block.hpp"

#include <cstring>

#include <silkworm/common/cast.hpp>
#include <silkworm/rlp/encode.hpp>

namespace silkworm {

evmc::bytes32 BlockHeader::hash(bool for_sealing) const {
    Bytes rlp;
    rlp::encode(rlp, *this, for_sealing);
    return bit_cast<evmc_bytes32>(keccak256(rlp));
}

bool operator==(const BlockHeader& a, const BlockHeader& b) {
    return a.parent_hash == b.parent_hash && a.ommers_hash == b.ommers_hash && a.beneficiary == b.beneficiary &&
           a.state_root == b.state_root && a.transactions_root == b.transactions_root &&
           a.receipts_root == b.receipts_root && a.logs_bloom == b.logs_bloom && a.difficulty == b.difficulty &&
           a.number == b.number && a.gas_limit == b.gas_limit && a.gas_used == b.gas_used &&
           a.timestamp == b.timestamp && a.extra_data == b.extra_data && a.mix_hash == b.mix_hash && a.nonce == b.nonce;
}

bool operator==(const BlockBody& a, const BlockBody& b) {
    return a.transactions == b.transactions && a.ommers == b.ommers;
}

void Block::recover_senders() {
    for (Transaction& txn : transactions) {
        txn.recover_sender();
    }
}

namespace rlp {

    // Computes the length of the RLP payload
    static Header rlp_header(const BlockHeader& header, bool for_sealing = false) {
        Header rlp_head{true, 0};
        rlp_head.payload_length += kHashLength + 1;                                        // parent_hash
        rlp_head.payload_length += kHashLength + 1;                                        // ommers_hash
        rlp_head.payload_length += kAddressLength + 1;                                     // beneficiary
        rlp_head.payload_length += kHashLength + 1;                                        // state_root
        rlp_head.payload_length += kHashLength + 1;                                        // transactions_root
        rlp_head.payload_length += kHashLength + 1;                                        // receipts_root
        rlp_head.payload_length += kBloomByteLength + length_of_length(kBloomByteLength);  // logs_bloom
        rlp_head.payload_length += length(header.difficulty);                              // difficulty
        rlp_head.payload_length += length(header.number);                                  // block height
        rlp_head.payload_length += length(header.gas_limit);                               // gas_limit
        rlp_head.payload_length += length(header.gas_used);                                // gas_used
        rlp_head.payload_length += length(header.timestamp);                               // timestamp
        rlp_head.payload_length += length(header.extra_data);                              // extra_data
        if (!for_sealing) {
            rlp_head.payload_length += kHashLength + 1;  // mix_hash
            rlp_head.payload_length += 8 + 1;            // nonce
        }
        return rlp_head;
    }

    size_t length(const BlockHeader& header) {
        Header rlp_head{rlp_header(header)};
        return length_of_length(rlp_head.payload_length) + rlp_head.payload_length;
    }

    void encode(Bytes& to, const BlockHeader& header, bool for_sealing) {
        encode_header(to, rlp_header(header, for_sealing));
        encode(to, header.parent_hash.bytes);
        encode(to, header.ommers_hash.bytes);
        encode(to, header.beneficiary.bytes);
        encode(to, header.state_root.bytes);
        encode(to, header.transactions_root.bytes);
        encode(to, header.receipts_root.bytes);
        encode(to, full_view(header.logs_bloom));
        encode(to, header.difficulty);
        encode(to, header.number);
        encode(to, header.gas_limit);
        encode(to, header.gas_used);
        encode(to, header.timestamp);
        encode(to, header.extra_data);
        if (!for_sealing) {
            encode(to, header.mix_hash.bytes);
            encode(to, header.nonce);
        }
    }

    template <>
    DecodingResult decode(ByteView& from, BlockHeader& to) noexcept {
        auto [rlp_head, err1]{decode_header(from)};
        if (err1 != DecodingResult::kOk) {
            return err1;
        }
        if (!rlp_head.list) {
            return DecodingResult::kUnexpectedString;
        }
        uint64_t leftover{from.length() - rlp_head.payload_length};

        if (DecodingResult err{decode(from, to.parent_hash.bytes)}; err != DecodingResult::kOk) {
            return err;
        }
        if (DecodingResult err{decode(from, to.ommers_hash.bytes)}; err != DecodingResult::kOk) {
            return err;
        }
        if (DecodingResult err{decode(from, to.beneficiary.bytes)}; err != DecodingResult::kOk) {
            return err;
        }
        if (DecodingResult err{decode(from, to.state_root.bytes)}; err != DecodingResult::kOk) {
            return err;
        }
        if (DecodingResult err{decode(from, to.transactions_root.bytes)}; err != DecodingResult::kOk) {
            return err;
        }
        if (DecodingResult err{decode(from, to.receipts_root.bytes)}; err != DecodingResult::kOk) {
            return err;
        }
        if (DecodingResult err{decode(from, to.logs_bloom)}; err != DecodingResult::kOk) {
            return err;
        }
        if (DecodingResult err{decode(from, to.difficulty)}; err != DecodingResult::kOk) {
            return err;
        }
        if (DecodingResult err{decode(from, to.number)}; err != DecodingResult::kOk) {
            return err;
        }
        if (DecodingResult err{decode(from, to.gas_limit)}; err != DecodingResult::kOk) {
            return err;
        }
        if (DecodingResult err{decode(from, to.gas_used)}; err != DecodingResult::kOk) {
            return err;
        }
        if (DecodingResult err{decode(from, to.timestamp)}; err != DecodingResult::kOk) {
            return err;
        }
        if (DecodingResult err{decode(from, to.extra_data)}; err != DecodingResult::kOk) {
            return err;
        }
        if (DecodingResult err{decode(from, to.mix_hash.bytes)}; err != DecodingResult::kOk) {
            return err;
        }
        if (DecodingResult err{decode(from, to.nonce)}; err != DecodingResult::kOk) {
            return err;
        }

        return from.length() == leftover ? DecodingResult::kOk : DecodingResult::kListLengthMismatch;
    }

    size_t length(const BlockBody& block_body) {
        Header rlp_head{true, 0};
        rlp_head.payload_length += length(block_body.transactions);
        rlp_head.payload_length += length(block_body.ommers);

        return length_of_length(rlp_head.payload_length) + rlp_head.payload_length;
    }

    void encode(Bytes& to, const BlockBody& block_body) {
        Header rlp_head{true, 0};
        rlp_head.payload_length += length(block_body.transactions);
        rlp_head.payload_length += length(block_body.ommers);
        encode_header(to, rlp_head);
        encode(to, block_body.transactions);
        encode(to, block_body.ommers);
    }

    template <>
    DecodingResult decode(ByteView& from, BlockBody& to) noexcept {
        auto [rlp_head, err]{decode_header(from)};
        if (err != DecodingResult::kOk) {
            return err;
        }
        if (!rlp_head.list) {
            return DecodingResult::kUnexpectedString;
        }
        uint64_t leftover{from.length() - rlp_head.payload_length};

        if (err = decode_vector(from, to.transactions); err != DecodingResult::kOk) {
            return err;
        }
        if (err = decode_vector(from, to.ommers); err != DecodingResult::kOk) {
            return err;
        }

        return from.length() == leftover ? DecodingResult::kOk : DecodingResult::kListLengthMismatch;
    }

    template <>
    DecodingResult decode(ByteView& from, Block& to) noexcept {
        auto [rlp_head, err]{decode_header(from)};
        if (err != DecodingResult::kOk) {
            return err;
        }
        if (!rlp_head.list) {
            return DecodingResult::kUnexpectedString;
        }
        uint64_t leftover{from.length() - rlp_head.payload_length};

        if (err = decode(from, to.header); err != DecodingResult::kOk) {
            return err;
        }
        if (err = decode_vector(from, to.transactions); err != DecodingResult::kOk) {
            return err;
        }
        if (err = decode_vector(from, to.ommers); err != DecodingResult::kOk) {
            return err;
        }

        return from.length() == leftover ? DecodingResult::kOk : DecodingResult::kListLengthMismatch;
    }

    size_t length(const Block& block) {
        Header rlp_head{true, 0};
        rlp_head.payload_length += length(block.header);
        rlp_head.payload_length += length(block.transactions);
        rlp_head.payload_length += length(block.ommers);

        return length_of_length(rlp_head.payload_length) + rlp_head.payload_length;
    }

    void encode(Bytes& to, const Block& block) {
        Header rlp_head{true, 0};
        rlp_head.payload_length += length(block.header);
        rlp_head.payload_length += length(block.transactions);
        rlp_head.payload_length += length(block.ommers);
        encode_header(to, rlp_head);
        encode(to, block.header);
        encode(to, block.transactions);
        encode(to, block.ommers);
    }

}  // namespace rlp
}  // namespace silkworm
