"""
Management command to export liquidation events with swap paths.

Queries the LiquidationCall event table and enriches each liquidation with:
- Path to convert collateral asset to debt asset (for repayment)
- Path to convert collateral asset to WETH (for profit taking)

Only includes liquidations where both collateral and debt assets are priority assets.

USAGE:
    # Export all liquidations
    python manage.py export_liquidations_with_paths

    # Export liquidations from last 7 days
    python manage.py export_liquidations_with_paths --days 7

    # Export to custom file
    python manage.py export_liquidations_with_paths --output my_liquidations.csv

    # Limit number of results
    python manage.py export_liquidations_with_paths --limit 1000

OUTPUT CSV COLUMNS:
    - user: User address being liquidated
    - collateral_asset: Collateral asset address
    - debt_asset: Debt asset address
    - debt_to_cover: Amount of debt covered in the liquidation
    - liquidated_collateral_amount: Amount of collateral liquidated
    - transaction_hash: Transaction hash
    - transaction_index: Transaction index in block
    - block_height: Block number
    - liquidator: Liquidator address
    - path_collateral_to_debt: Swap path to convert collateral to debt (comma-separated pools)
    - path_collateral_to_weth: Swap path to convert collateral to WETH (comma-separated pools)
"""

import csv
import logging

from django.core.management.base import BaseCommand

from utils.clickhouse.client import clickhouse_client

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = (
        "Export liquidation events with swap paths for collateral-to-debt "
        "and collateral-to-WETH conversions"
    )

    # Priority assets (lowercase for consistent lookups)
    PRIORITY_ASSETS = {
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
        "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
        "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",  # WBTC
    }

    WETH_ADDRESS = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

    def add_arguments(self, parser):
        parser.add_argument(
            "--output",
            type=str,
            default="liquidations_with_paths.csv",
            help="Output CSV file path (default: liquidations_with_paths.csv)",
        )
        parser.add_argument(
            "--days",
            type=int,
            help="Only export liquidations from last N days (default: all time)",
        )
        parser.add_argument(
            "--limit",
            type=int,
            help="Limit number of results (default: no limit)",
        )

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("=" * 80))
        self.stdout.write(self.style.SUCCESS("Export Liquidations with Swap Paths"))
        self.stdout.write(self.style.SUCCESS("=" * 80))

        output_file = options["output"]
        days = options.get("days")
        limit = options.get("limit")

        try:
            # Query liquidations
            liquidations = self._query_liquidations(days, limit)

            if not liquidations:
                self.stdout.write(
                    self.style.WARNING("No liquidations found matching criteria")
                )
                return

            self.stdout.write(f"\nFound {len(liquidations)} liquidation(s) to process")

            # Enrich with swap paths
            enriched = self._enrich_with_paths(liquidations)

            # Export to CSV
            self._export_to_csv(enriched, output_file)

            self.stdout.write("\n" + "=" * 80)
            self.stdout.write(
                self.style.SUCCESS(
                    f"✅ Successfully exported {len(enriched)} liquidations to {output_file}"
                )
            )
            self.stdout.write("=" * 80)

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"\n❌ Error: {e}"))
            import traceback

            traceback.print_exc()

    def _query_liquidations(self, days=None, limit=None):
        """Query liquidation events from ClickHouse."""
        self.stdout.write("\n📊 Querying liquidation events...")

        # Build time filter
        time_filter = ""
        if days:
            time_filter = f"AND l.blockTimestamp >= now() - INTERVAL {days} DAY"

        # Build priority assets filter (only liquidations with priority assets)
        priority_assets_list = ", ".join([f"'{addr}'" for addr in self.PRIORITY_ASSETS])
        asset_filter = f"""
            AND lower(l.collateralAsset) IN ({priority_assets_list})
            AND lower(l.debtAsset) IN ({priority_assets_list})
        """

        # Build limit clause
        limit_clause = f"LIMIT {limit}" if limit else ""

        query = f"""
        SELECT
            l.user,
            lower(l.collateralAsset) as collateral_asset,
            lower(l.debtAsset) as debt_asset,
            l.debtToCover as debt_to_cover,
            l.liquidatedCollateralAmount as liquidated_collateral_amount,
            l.transactionHash as transaction_hash,
            l.transactionIndex as transaction_index,
            l.blockNumber as block_height,
            l.liquidator
        FROM aave_ethereum.LiquidationCall l
        WHERE 1=1
            {time_filter}
            {asset_filter}
        ORDER BY l.blockNumber DESC, l.transactionIndex DESC
        {limit_clause}
        """

        result = clickhouse_client.execute_query(query)

        liquidations = []
        if result.result_rows:
            for row in result.result_rows:
                liquidations.append(
                    {
                        "user": row[0],
                        "collateral_asset": row[1],
                        "debt_asset": row[2],
                        "debt_to_cover": str(row[3]),
                        "liquidated_collateral_amount": str(row[4]),
                        "transaction_hash": row[5],
                        "transaction_index": row[6],
                        "block_height": row[7],
                        "liquidator": row[8],
                    }
                )

        self.stdout.write(f"  Found {len(liquidations)} liquidation(s)")
        return liquidations

    def _enrich_with_paths(self, liquidations):
        """Enrich liquidations with swap paths from ClickHouse dictionary."""
        self.stdout.write("\n🛣️  Looking up swap paths...")

        enriched = []
        paths_found = 0
        paths_missing = 0

        for liq in liquidations:
            collateral = liq["collateral_asset"]
            debt = liq["debt_asset"]

            # Get path: collateral -> debt
            path_to_debt = self._get_swap_path(collateral, debt)

            # Get path: collateral -> WETH
            path_to_weth = None
            if collateral.lower() != self.WETH_ADDRESS:
                path_to_weth = self._get_swap_path(collateral, self.WETH_ADDRESS)

            # Track statistics
            if path_to_debt:
                paths_found += 1
            else:
                paths_missing += 1

            enriched.append(
                {
                    **liq,
                    "path_collateral_to_debt": path_to_debt or "NO_PATH",
                    "path_collateral_to_weth": path_to_weth or "NO_PATH"
                    if collateral.lower() != self.WETH_ADDRESS
                    else "N/A",
                }
            )

        self.stdout.write(f"  Paths found: {paths_found}")
        self.stdout.write(f"  Paths missing: {paths_missing}")

        return enriched

    def _get_swap_path(self, token_in, token_out):
        """
        Get swap path from ClickHouse dictionary.

        Args:
            token_in: Source token address (lowercase)
            token_out: Destination token address (lowercase)

        Returns:
            Comma-separated pool addresses, or None if not found
        """
        # Skip if same token
        if token_in.lower() == token_out.lower():
            return None

        try:
            query = f"""
                SELECT dictGet('aave_ethereum.dict_swap_paths', 'path',
                               ('{token_in.lower()}', '{token_out.lower()}'))
            """

            result = clickhouse_client.execute_query(query)

            if result.result_rows and result.result_rows[0][0]:
                path = result.result_rows[0][0]
                # Empty string means not found in dictionary
                if path and path != "":
                    return path

            return None

        except Exception as e:
            logger.warning(f"Error getting swap path {token_in} -> {token_out}: {e}")
            return None

    def _export_to_csv(self, data, output_file):
        """Export enriched liquidations to CSV."""
        self.stdout.write(f"\n📝 Writing to {output_file}...")

        if not data:
            self.stdout.write(self.style.WARNING("  No data to export"))
            return

        # Define CSV columns
        fieldnames = [
            "user",
            "collateral_asset",
            "debt_asset",
            "debt_to_cover",
            "liquidated_collateral_amount",
            "transaction_hash",
            "transaction_index",
            "block_height",
            "liquidator",
            "path_collateral_to_debt",
            "path_collateral_to_weth",
        ]

        with open(output_file, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

        self.stdout.write(f"  Wrote {len(data)} rows")
