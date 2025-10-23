"""
Management command to find the best 1-hop and 2-hop swap paths between priority assets.

This command uses the pools discovered by find_priority_asset_pools and finds optimal
swap routes using Uniswap V3 Quoter to get real output amounts.

USAGE:
    # Find best path to swap 1 WETH to USDC
    python manage.py find_swap_path --from WETH --to USDC --amount 1

    # Find path using asset addresses
    python manage.py find_swap_path \\
        --from 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2 \\
        --to 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 \\
        --amount 1

    # Find path with custom amount (in human-readable units)
    python manage.py find_swap_path --from WBTC --to USDT --amount 0.5

    # Use custom pools file
    python manage.py find_swap_path --from WETH --to USDC --amount 1 --pools custom_pools.json

    # Show all paths (not just the best)
    python manage.py find_swap_path --from WETH --to USDC --amount 1 --show-all

WHAT IT DOES:
    1. Loads pool data from pools.json
    2. Builds a graph of available swap routes
    3. Finds all 1-hop paths (direct swaps)
    4. Finds all 2-hop paths (swaps through an intermediate asset)
    5. Gets real quotes from Uniswap V3 Quoter contract
    6. Compares paths and shows the best route(s)

OUTPUT:
    - All possible paths (1-hop and 2-hop)
    - Expected output amount for each path
    - Fee tiers used
    - Pool addresses
    - Price impact comparison
    - Recommendation for best path
"""

import json
import logging
import os
from collections import defaultdict
from typing import Dict, List, Optional

from django.core.management.base import BaseCommand
from web3 import Web3

from utils.interfaces.base import BaseContractInterface

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = (
        "Find the best 1-hop and 2-hop swap paths between priority assets "
        "using pools from pools.json. Queries Uniswap V3 Quoter for real output amounts."
    )

    # Uniswap V3 Quoter V2 on Ethereum mainnet
    QUOTER_V2_ADDRESS = "0x61fFE014bA17989E743c5F6cB21bF9697530B21e"

    # Token decimals (standard for these assets) - stored in lowercase for case-insensitive lookup
    TOKEN_DECIMALS = {
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": 18,  # WETH
        "0xdac17f958d2ee523a2206206994597c13d831ec7": 6,  # USDT
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": 6,  # USDC
        "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": 8,  # WBTC
    }

    def add_arguments(self, parser):
        parser.add_argument(
            "--from",
            dest="from_asset",
            type=str,
            required=True,
            help="Source asset (symbol like 'WETH' or address)",
        )
        parser.add_argument(
            "--to",
            dest="to_asset",
            type=str,
            required=True,
            help="Destination asset (symbol like 'USDC' or address)",
        )
        parser.add_argument(
            "--amount",
            type=float,
            required=True,
            help="Amount to swap (in human-readable units, e.g., 1.5 for 1.5 WETH)",
        )
        parser.add_argument(
            "--pools",
            type=str,
            default="payments/pools.json",
            help="Path to pools JSON file (default: payments/pools.json)",
        )
        parser.add_argument(
            "--show-all",
            action="store_true",
            help="Show all paths, not just the best one",
        )

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("=" * 80))
        self.stdout.write(self.style.SUCCESS("Swap Path Finder - Uniswap V3"))
        self.stdout.write(self.style.SUCCESS("=" * 80))

        from_asset = options["from_asset"]
        to_asset = options["to_asset"]
        amount = options["amount"]
        pools_file = options["pools"]
        show_all = options["show_all"]

        try:
            # Load pools data
            pool_data = self._load_pools(pools_file)

            # Resolve asset addresses
            from_addr = self._resolve_asset(from_asset, pool_data)
            to_addr = self._resolve_asset(to_asset, pool_data)

            # Get symbols
            from_symbol = self._get_symbol(from_addr, pool_data)
            to_symbol = self._get_symbol(to_addr, pool_data)

            # Convert amount to raw units
            from_decimals = self.TOKEN_DECIMALS.get(from_addr.lower())
            if not from_decimals:
                raise ValueError(f"Unknown decimals for {from_addr}")

            amount_raw = int(amount * (10**from_decimals))

            # Display swap details
            self._display_swap_info(
                from_symbol, to_symbol, from_addr, to_addr, amount, amount_raw
            )

            # Build graph from pools
            graph = self._build_graph(pool_data["pools"])

            # Find all paths
            one_hop_paths = self._find_one_hop_paths(from_addr, to_addr, graph)
            two_hop_paths = self._find_two_hop_paths(from_addr, to_addr, graph)

            self.stdout.write(
                f"\nFound {len(one_hop_paths)} one-hop path(s) and {len(two_hop_paths)} two-hop path(s)"
            )

            if not one_hop_paths and not two_hop_paths:
                self.stdout.write(
                    self.style.ERROR(
                        "\n❌ No paths found between these assets using available pools"
                    )
                )
                return

            # Initialize Quoter interface
            self.quoter = BaseContractInterface(self.QUOTER_V2_ADDRESS)

            # Get quotes for all paths
            all_paths = []

            self.stdout.write("\n" + "=" * 80)
            self.stdout.write(self.style.SUCCESS("💱 Getting Quotes..."))
            self.stdout.write("=" * 80 + "\n")

            # Quote 1-hop paths
            for path_info in one_hop_paths:
                quote = self._quote_exact_input_single(
                    path_info["token_in"],
                    path_info["token_out"],
                    amount_raw,
                    path_info["fee"],
                    path_info["pool_address"],
                )
                if quote:
                    path_info["output_amount"] = quote["amount_out"]
                    path_info["hops"] = 1
                    all_paths.append(path_info)

            # Quote 2-hop paths
            for path_info in two_hop_paths:
                quote = self._quote_exact_input_multi_hop(
                    [
                        path_info["token_in"],
                        path_info["intermediate"],
                        path_info["token_out"],
                    ],
                    [path_info["fee1"], path_info["fee2"]],
                    amount_raw,
                )
                if quote:
                    path_info["output_amount"] = quote["amount_out"]
                    path_info["hops"] = 2
                    all_paths.append(path_info)

            if not all_paths:
                self.stdout.write(
                    self.style.ERROR(
                        "\n❌ No valid quotes received. Pools may have insufficient liquidity."
                    )
                )
                return

            # Sort by output amount (best first)
            all_paths.sort(key=lambda x: x["output_amount"], reverse=True)

            # Display results
            to_decimals = self.TOKEN_DECIMALS.get(to_addr.lower())
            self._display_paths(all_paths, to_symbol, to_decimals, show_all)

            # Show recommendation
            self._display_recommendation(all_paths[0], to_symbol, to_decimals)

            self.stdout.write("\n" + "=" * 80)
            self.stdout.write(self.style.SUCCESS("✅ Path Finding Completed!"))
            self.stdout.write("=" * 80)

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"\n❌ Error: {e}"))
            import traceback

            traceback.print_exc()

    def _load_pools(self, pools_file: str) -> Dict:
        """Load pools data from JSON file."""
        if not os.path.exists(pools_file):
            raise FileNotFoundError(
                f"Pools file not found: {pools_file}. "
                "Run 'python manage.py find_priority_asset_pools --output pools.json' first."
            )

        with open(pools_file, "r") as f:
            return json.load(f)

    def _resolve_asset(self, asset: str, pool_data: Dict) -> str:
        """
        Resolve asset symbol or address to checksummed address.

        Args:
            asset: Symbol (e.g., 'WETH') or address
            pool_data: Pool data dictionary

        Returns:
            Checksummed address
        """
        # Check if it's already an address
        if asset.startswith("0x"):
            return Web3.to_checksum_address(asset)

        # Look up symbol in priority assets (case-insensitive)
        symbol_upper = asset.upper()
        for sym, addr in pool_data["priority_assets"].items():
            if sym.upper() == symbol_upper:
                return Web3.to_checksum_address(addr)

        raise ValueError(
            f"Unknown asset: {asset}. Must be a symbol (WETH, USDC, USDT, WBTC) or address."
        )

    def _get_symbol(self, address: str, pool_data: Dict) -> str:
        """Get symbol for an address."""
        addr_lower = address.lower()
        for symbol, addr in pool_data["priority_assets"].items():
            if addr.lower() == addr_lower:
                return symbol
        return address[:10] + "..."

    def _display_swap_info(
        self,
        from_symbol: str,
        to_symbol: str,
        from_addr: str,
        to_addr: str,
        amount: float,
        amount_raw: int,
    ):
        """Display swap information."""
        self.stdout.write("\n📊 Swap Details:")
        self.stdout.write("-" * 80)
        self.stdout.write(f"  From:   {from_symbol} ({from_addr})")
        self.stdout.write(f"  To:     {to_symbol} ({to_addr})")
        self.stdout.write(f"  Amount: {amount:,.6f} {from_symbol} ({amount_raw} raw)")

    def _build_graph(self, pools: List[Dict]) -> Dict:
        """
        Build a graph representation of pools for pathfinding.

        Returns:
            Dict mapping (token_address) -> List of (neighbor_address, pool_info)
        """
        graph = defaultdict(list)

        for pool in pools:
            if not pool.get("has_liquidity"):
                continue

            token0 = pool["token0"].lower()
            token1 = pool["token1"].lower()

            # Both directions
            graph[token0].append(
                {
                    "neighbor": token1,
                    "pool_address": pool["pool_address"],
                    "fee": pool["fee"],
                    "symbol": pool["symbol1"],
                }
            )
            graph[token1].append(
                {
                    "neighbor": token0,
                    "pool_address": pool["pool_address"],
                    "fee": pool["fee"],
                    "symbol": pool["symbol0"],
                }
            )

        return graph

    def _find_one_hop_paths(
        self, from_addr: str, to_addr: str, graph: Dict
    ) -> List[Dict]:
        """Find all 1-hop (direct) paths."""
        from_lower = from_addr.lower()
        to_lower = to_addr.lower()

        paths = []
        if from_lower in graph:
            for edge in graph[from_lower]:
                if edge["neighbor"] == to_lower:
                    paths.append(
                        {
                            "token_in": from_addr,
                            "token_out": to_addr,
                            "pool_address": edge["pool_address"],
                            "fee": edge["fee"],
                            "symbol_out": edge["symbol"],
                        }
                    )

        return paths

    def _find_two_hop_paths(
        self, from_addr: str, to_addr: str, graph: Dict
    ) -> List[Dict]:
        """Find all 2-hop paths through an intermediate asset."""
        from_lower = from_addr.lower()
        to_lower = to_addr.lower()

        paths = []
        if from_lower not in graph:
            return paths

        # For each neighbor of from_addr
        for first_hop in graph[from_lower]:
            intermediate = first_hop["neighbor"]

            # Skip if intermediate is the destination
            if intermediate == to_lower:
                continue

            # Check if intermediate connects to destination
            if intermediate in graph:
                for second_hop in graph[intermediate]:
                    if second_hop["neighbor"] == to_lower:
                        paths.append(
                            {
                                "token_in": from_addr,
                                "intermediate": Web3.to_checksum_address(intermediate),
                                "token_out": to_addr,
                                "pool1_address": first_hop["pool_address"],
                                "pool2_address": second_hop["pool_address"],
                                "fee1": first_hop["fee"],
                                "fee2": second_hop["fee"],
                                "intermediate_symbol": first_hop["symbol"],
                            }
                        )

        return paths

    def _quote_exact_input_single(
        self,
        token_in: str,
        token_out: str,
        amount_in: int,
        fee: int,
        pool_address: str,
    ) -> Optional[Dict]:
        """
        Get quote for a single-hop swap using Quoter V2.

        Args:
            token_in: Input token address
            token_out: Output token address
            amount_in: Input amount (raw units)
            fee: Fee tier
            pool_address: Pool address

        Returns:
            Dict with amount_out, or None if quote fails
        """
        try:
            # QuoteExactInputSingle params struct:
            # (address tokenIn, address tokenOut, uint256 amountIn, uint24 fee, uint160 sqrtPriceLimitX96)
            call_data = {
                "method_signature": "quoteExactInputSingle((address,address,uint256,uint24,uint160))",
                "param_types": ["(address,address,uint256,uint24,uint160)"],
                "params": [
                    (
                        Web3.to_checksum_address(token_in),
                        Web3.to_checksum_address(token_out),
                        amount_in,
                        fee,
                        0,  # sqrtPriceLimitX96 = 0 means no limit
                    )
                ],
            }

            result = self.quoter.batch_eth_call([call_data])

            if isinstance(result, list) and len(result) > 0:
                output_hex = result[0].get("result", "0x")
                if output_hex and output_hex != "0x":
                    # Decode the result - quoter returns (uint256 amountOut, ...)
                    # First 32 bytes is amountOut
                    amount_out = int(output_hex[:66], 16)
                    return {"amount_out": amount_out}

        except Exception as e:
            logger.warning(f"Quote failed for pool {pool_address}: {e}")

        return None

    def _quote_exact_input_multi_hop(
        self, path_tokens: List[str], path_fees: List[int], amount_in: int
    ) -> Optional[Dict]:
        """
        Get quote for a multi-hop swap using Quoter V2.

        Args:
            path_tokens: List of token addresses [tokenIn, intermediate, tokenOut]
            path_fees: List of fee tiers [fee1, fee2]
            amount_in: Input amount (raw units)

        Returns:
            Dict with amount_out, or None if quote fails
        """
        try:
            # Encode path: token0 + fee0 + token1 + fee1 + token2
            # Each address is 20 bytes, each fee is 3 bytes (uint24)
            path_bytes = b""
            for i, token in enumerate(path_tokens):
                # Add token address (20 bytes)
                token_addr = token[2:] if token.startswith("0x") else token
                path_bytes += bytes.fromhex(token_addr.lower())
                # Add fee if not last token (3 bytes)
                if i < len(path_fees):
                    path_bytes += path_fees[i].to_bytes(3, "big")

            # Pass as bytes object, not hex string
            # quoteExactInput(bytes path, uint256 amountIn)
            call_data = {
                "method_signature": "quoteExactInput(bytes,uint256)",
                "param_types": ["bytes", "uint256"],
                "params": [path_bytes, amount_in],
            }

            result = self.quoter.batch_eth_call([call_data])

            if isinstance(result, list) and len(result) > 0:
                output_hex = result[0].get("result", "0x")
                if output_hex and output_hex != "0x":
                    # First 32 bytes is amountOut
                    amount_out = int(output_hex[:66], 16)
                    return {"amount_out": amount_out}

        except Exception as e:
            logger.warning(f"Multi-hop quote failed: {e}")

        return None

    def _display_paths(
        self, paths: List[Dict], to_symbol: str, to_decimals: int, show_all: bool
    ):
        """Display found paths with quotes."""
        self.stdout.write("\n" + "=" * 80)
        self.stdout.write(self.style.SUCCESS("🛣️  Swap Paths (Ranked by Output)"))
        self.stdout.write("=" * 80 + "\n")

        paths_to_show = paths if show_all else [paths[0]]

        for i, path in enumerate(paths_to_show, 1):
            output_human = path["output_amount"] / (10**to_decimals)

            rank_emoji = (
                "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "  "
            )

            self.stdout.write(f"{rank_emoji} Path #{i} ({path['hops']}-hop):")
            self.stdout.write(f"  Output: {output_human:,.6f} {to_symbol}")

            if path["hops"] == 1:
                self.stdout.write("  Route:  Direct swap")
                self.stdout.write(f"  Pool:   {path['pool_address']}")
                self.stdout.write(f"  Fee:    {path['fee'] / 10000}%")
            else:
                self.stdout.write(
                    f"  Route:  Via {path['intermediate_symbol']} ({path['intermediate'][:10]}...)"
                )
                self.stdout.write(
                    f"  Pool 1: {path['pool1_address']} (fee: {path['fee1'] / 10000}%)"
                )
                self.stdout.write(
                    f"  Pool 2: {path['pool2_address']} (fee: {path['fee2'] / 10000}%)"
                )

            # Calculate price impact if not the best path
            if i > 1:
                best_output = paths[0]["output_amount"]
                price_impact = (best_output - path["output_amount"]) / best_output * 100
                self.stdout.write(
                    self.style.WARNING(f"  Impact: -{price_impact:.2f}% vs best path")
                )

            self.stdout.write("")

    def _display_recommendation(
        self, best_path: Dict, to_symbol: str, to_decimals: int
    ):
        """Display recommendation for the best path."""
        self.stdout.write("\n" + "=" * 80)
        self.stdout.write(self.style.SUCCESS("💡 Recommendation"))
        self.stdout.write("=" * 80 + "\n")

        output_human = best_path["output_amount"] / (10**to_decimals)

        self.stdout.write(
            f"Best path: {best_path['hops']}-hop swap yielding {output_human:,.6f} {to_symbol}"
        )

        if best_path["hops"] == 1:
            self.stdout.write(f"Use pool: {best_path['pool_address']}")
            self.stdout.write(f"Fee tier: {best_path['fee'] / 10000}%")
            self.stdout.write("\nExecution: Direct swap through Uniswap V3 Router")
        else:
            self.stdout.write(
                f"Route through: {best_path['intermediate_symbol']} ({best_path['intermediate']})"
            )
            self.stdout.write(
                f"Pool 1: {best_path['pool1_address']} (fee: {best_path['fee1'] / 10000}%)"
            )
            self.stdout.write(
                f"Pool 2: {best_path['pool2_address']} (fee: {best_path['fee2'] / 10000}%)"
            )
            self.stdout.write(
                "\nExecution: Multi-hop swap through Uniswap V3 Router using encoded path"
            )
