#!/bin/bash
# Get transaction details from block and index
# Usage: ./get_tx.sh <block_number> <tx_index>

BLOCK=$1
TX_INDEX=$2
RPC_URL="https://reth-ethereum.ithaca.xyz/rpc"

# Convert block number to hex
BLOCK_HEX=$(printf "0x%x" $BLOCK)
TX_INDEX_HEX=$(printf "0x%x" $TX_INDEX)

# Get transaction by block number and index and output the JSON
cast rpc eth_getTransactionByBlockNumberAndIndex $BLOCK_HEX $TX_INDEX_HEX --rpc-url $RPC_URL 2>/dev/null
