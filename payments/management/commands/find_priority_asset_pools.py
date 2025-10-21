"""
Management command to identify Uniswap V3 pool addresses for priority liquidation assets.

This command finds pool addresses for all pairs of priority assets (WETH, USDT, USDC, WBTC)
that can be used for one-hop swaps during liquidation execution.

PRIORITY ASSETS (used in /liquidation-candidates/ template):
    - WETH:  0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2
    - USDT:  0xdAC17F958D2ee523a2206206994597C13D831ec7
    - USDC:  0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48
    - WBTC:  0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599

USAGE:
    # Find all pool addresses for priority assets
    python manage.py find_priority_asset_pools

    # Show only pools with significant liquidity
    python manage.py find_priority_asset_pools --min-liquidity 100000

    # Export pool addresses to a file
    python manage.py find_priority_asset_pools --output pools.json

WHAT IT DOES:
    1. Queries Uniswap V3 Factory contract to find pools for each asset pair
    2. Checks all three fee tiers (0.05%, 0.3%, 1%)
    3. Retrieves pool liquidity and other metrics
    4. Displays formatted results with pool addresses and metadata
    5. Optionally exports data for use in liquidation execution

OUTPUT INCLUDES:
    - Pool addresses for each pair (A->B and B->A use same pool)
    - Fee tier (500 = 0.05%, 3000 = 0.3%, 10000 = 1%)
    - Pool liquidity (TVL)
    - Token0 and Token1 (ordered by address)
    - Whether the pool is actively used
"""

import json
import logging
from itertools import combinations
from typing import Dict, List

from django.core.management.base import BaseCommand
from web3 import Web3

from utils.interfaces.base import BaseContractInterface

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = (
        "Find Uniswap V3 pool addresses for the 4 priority assets used in liquidation-candidates. "
        "Identifies pools for one-hop swaps between WETH, USDT, USDC, and WBTC."
    )

    # Priority assets from /liquidation-candidates/ template (line 262)
    PRIORITY_ASSETS = {
        "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "WBTC": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
    }

    # Uniswap V3 Factory contract on Ethereum mainnet
    UNISWAP_V3_FACTORY = "0x1F98431c8aD98523631AE4a59f267346ea31F984"

    # Fee tiers (in basis points)
    FEE_TIERS = [500, 3000, 10000]  # 0.05%, 0.3%, 1%

    def add_arguments(self, parser):
        parser.add_argument(
            "--min-liquidity",
            type=float,
            default=0,
            help="Minimum pool liquidity in USD (default: 0, show all pools)",
        )
        parser.add_argument(
            "--output",
            type=str,
            help="Export pool data to JSON file (e.g., pools.json)",
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Show detailed information including empty pools",
        )

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("=" * 80))
        self.stdout.write(self.style.SUCCESS("Priority Asset Pool Finder - Uniswap V3"))
        self.stdout.write(self.style.SUCCESS("=" * 80))

        min_liquidity = options["min_liquidity"]
        verbose = options["verbose"]
        output_file = options.get("output")

        try:
            # Initialize Web3 interface
            self.interface = BaseContractInterface(self.UNISWAP_V3_FACTORY)

            # Display priority assets
            self._display_priority_assets()

            # Find all pools
            self.stdout.write("\n" + "=" * 80)
            self.stdout.write(self.style.SUCCESS("🔍 Searching for Pools..."))
            self.stdout.write("=" * 80 + "\n")

            pool_data = self._find_all_pools()

            # Filter by liquidity if requested
            if min_liquidity > 0:
                pool_data = self._filter_by_liquidity(pool_data, min_liquidity)

            # Display results
            self._display_pools(pool_data, verbose)

            # Export if requested
            if output_file:
                self._export_pools(pool_data, output_file)

            # Summary
            self._display_summary(pool_data)

            self.stdout.write("\n" + "=" * 80)
            self.stdout.write(self.style.SUCCESS("✅ Pool Discovery Completed!"))
            self.stdout.write("=" * 80)

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"\n❌ Error: {e}"))
            import traceback

            traceback.print_exc()

    def _display_priority_assets(self):
        """Display the 5 priority assets used in liquidation-candidates."""
        self.stdout.write("\n📋 Priority Assets (from /liquidation-candidates/):")
        self.stdout.write("-" * 80)
        for symbol, address in self.PRIORITY_ASSETS.items():
            self.stdout.write(f"  {symbol:<8} {address}")

    def _find_all_pools(self) -> List[Dict]:
        """
        Find all Uniswap V3 pools for priority asset pairs.

        Returns:
            List of pool dictionaries with metadata
        """
        pool_data = []

        # Generate all unique pairs
        asset_pairs = list(combinations(self.PRIORITY_ASSETS.items(), 2))
        total_combinations = len(asset_pairs) * len(self.FEE_TIERS)

        self.stdout.write(
            f"Checking {len(asset_pairs)} asset pairs × {len(self.FEE_TIERS)} fee tiers "
            f"= {total_combinations} combinations\n"
        )

        for (symbol1, addr1), (symbol2, addr2) in asset_pairs:
            # Ensure token0 < token1 (Uniswap convention)
            if addr1.lower() < addr2.lower():
                token0, token1 = addr1, addr2
                symbol0, symbol1_name = symbol1, symbol2
            else:
                token0, token1 = addr2, addr1
                symbol0, symbol1_name = symbol2, symbol1

            for fee in self.FEE_TIERS:
                pool_address = self._get_pool_address(token0, token1, fee)

                if (
                    pool_address
                    and pool_address != "0x0000000000000000000000000000000000000000"
                ):
                    pool_info = self._get_pool_info(
                        pool_address, token0, token1, symbol0, symbol1_name, fee
                    )
                    pool_data.append(pool_info)
                    self.stdout.write(
                        f"  ✓ Found: {symbol0}/{symbol1_name} @ {fee / 10000}% fee"
                    )

        return pool_data

    def _get_pool_address(self, token0: str, token1: str, fee: int) -> str:
        """
        Get pool address from Uniswap V3 Factory.

        Args:
            token0: First token address (must be < token1)
            token1: Second token address
            fee: Fee tier in basis points

        Returns:
            Pool address or zero address if pool doesn't exist
        """
        try:
            # getPool(address,address,uint24) returns address
            call_data = {
                "method_signature": "getPool(address,address,uint24)",
                "param_types": ["address", "address", "uint24"],
                "params": [
                    Web3.to_checksum_address(token0),
                    Web3.to_checksum_address(token1),
                    fee,
                ],
            }

            result = self.interface.batch_eth_call([call_data])

            if isinstance(result, list) and len(result) > 0:
                pool_hex = result[0].get("result", "0x")
                # Decode address from result
                if pool_hex and pool_hex != "0x":
                    return Web3.to_checksum_address("0x" + pool_hex[-40:])

        except Exception as e:
            logger.error(f"Error getting pool address: {e}")

        return "0x0000000000000000000000000000000000000000"

    def _get_pool_info(
        self,
        pool_address: str,
        token0: str,
        token1: str,
        symbol0: str,
        symbol1: str,
        fee: int,
    ) -> Dict:
        """
        Get pool information including liquidity.

        Args:
            pool_address: Pool contract address
            token0: First token address
            token1: Second token address
            symbol0: First token symbol
            symbol1: Second token symbol
            fee: Fee tier

        Returns:
            Dictionary with pool metadata
        """
        try:
            # Use the factory interface to call liquidity() on the pool
            # We call it directly without needing the pool's ABI
            liquidity_call = {
                "method_signature": "liquidity()",
                "param_types": [],
                "params": [],
                "to": pool_address,  # Override to call the pool contract
            }

            result = self.interface.batch_eth_call([liquidity_call])

            liquidity = 0
            if isinstance(result, list) and len(result) > 0:
                liquidity_hex = result[0].get("result", "0x0")
                liquidity = int(liquidity_hex, 16) if liquidity_hex else 0

            return {
                "pool_address": pool_address,
                "token0": token0,
                "token1": token1,
                "symbol0": symbol0,
                "symbol1": symbol1,
                "fee": fee,
                "fee_percent": fee / 10000,
                "liquidity": liquidity,
                "has_liquidity": liquidity > 0,
            }

        except Exception as e:
            logger.warning(f"Error getting pool info for {pool_address}: {e}")
            return {
                "pool_address": pool_address,
                "token0": token0,
                "token1": token1,
                "symbol0": symbol0,
                "symbol1": symbol1,
                "fee": fee,
                "fee_percent": fee / 10000,
                "liquidity": 0,
                "has_liquidity": False,
            }

    def _filter_by_liquidity(
        self, pool_data: List[Dict], min_liquidity: float
    ) -> List[Dict]:
        """Filter pools by minimum liquidity."""
        filtered = [p for p in pool_data if p.get("liquidity", 0) >= min_liquidity]
        self.stdout.write(
            f"\nFiltered to {len(filtered)} pools with liquidity >= {min_liquidity:,.0f}"
        )
        return filtered

    def _display_pools(self, pool_data: List[Dict], verbose: bool):
        """Display pool information in formatted tables."""
        self.stdout.write("\n" + "=" * 80)
        self.stdout.write(self.style.SUCCESS("📊 Pool Addresses"))
        self.stdout.write("=" * 80 + "\n")

        if not pool_data:
            self.stdout.write("No pools found.")
            return

        # Sort by symbol pair and fee
        pool_data.sort(key=lambda x: (x["symbol0"], x["symbol1"], x["fee"]))

        # Display pools
        self.stdout.write(
            f"{'Pair':<15} {'Fee':<8} {'Pool Address':<44} {'Liquidity':<15}"
        )
        self.stdout.write("-" * 80)

        for pool in pool_data:
            if not verbose and not pool["has_liquidity"]:
                continue

            pair = f"{pool['symbol0']}/{pool['symbol1']}"
            fee_str = f"{pool['fee_percent']:.2f}%"
            liquidity_str = (
                f"{pool['liquidity']:,.0f}" if pool["has_liquidity"] else "No liquidity"
            )

            self.stdout.write(
                f"{pair:<15} {fee_str:<8} {pool['pool_address']:<44} {liquidity_str:<15}"
            )

    def _display_summary(self, pool_data: List[Dict]):
        """Display summary statistics."""
        self.stdout.write("\n" + "=" * 80)
        self.stdout.write(self.style.SUCCESS("📈 Summary"))
        self.stdout.write("=" * 80 + "\n")

        total_pools = len(pool_data)
        pools_with_liquidity = sum(1 for p in pool_data if p["has_liquidity"])
        total_liquidity = sum(p.get("liquidity", 0) for p in pool_data)

        # Count pools by fee tier
        fee_counts = {}
        for pool in pool_data:
            fee = pool["fee_percent"]
            fee_counts[fee] = fee_counts.get(fee, 0) + 1

        self.stdout.write(f"Total pools found:          {total_pools}")
        self.stdout.write(f"Pools with liquidity:       {pools_with_liquidity}")
        self.stdout.write(f"Total liquidity (raw):      {total_liquidity:,.0f}")
        self.stdout.write("\nPools by fee tier:")
        for fee, count in sorted(fee_counts.items()):
            self.stdout.write(f"  {fee:.2f}% fee: {count} pools")

        # Asset pair coverage
        unique_pairs = set(
            (p["symbol0"], p["symbol1"]) for p in pool_data if p["has_liquidity"]
        )
        self.stdout.write(f"\nUnique pairs with liquidity: {len(unique_pairs)}")

    def _export_pools(self, pool_data: List[Dict], output_file: str):
        """Export pool data to JSON file."""
        try:
            with open(output_file, "w") as f:
                json.dump(
                    {
                        "priority_assets": self.PRIORITY_ASSETS,
                        "pools": pool_data,
                        "factory_address": self.UNISWAP_V3_FACTORY,
                        "chain": "ethereum",
                    },
                    f,
                    indent=2,
                )
            self.stdout.write(
                self.style.SUCCESS(f"\n✓ Pool data exported to {output_file}")
            )
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"\n❌ Failed to export: {e}"))
