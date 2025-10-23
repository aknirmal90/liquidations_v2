"""
Management command to test liquidation detection by manually setting predicted transaction prices.

This command allows you to simulate price updates for specific assets and trigger the
liquidation detection pipeline to test the system end-to-end.

WORKFLOW:
This command executes the complete liquidation detection pipeline:
1. Displays planned price updates (if provided)
2. Shows current system state (health factors, existing candidates)
3. Calls InsertTransactionNumeratorTask which:
   - Inserts price data into TransactionRawNumerator table
   - Triggers EstimateFutureLiquidationCandidatesTask
4. Detection task identifies users with HF > 1 currently but HF < 1 with predicted prices
5. Stores all detections in LiquidationDetections table
6. Sends SimplePush notifications
7. Displays results summary with verification commands

Note: All data written is permanent. Liquidation execution (MEV) is on a different branch.

USAGE EXAMPLES:

    # List all available assets with their current prices
    python manage.py test_liquidation_detection --list-assets

    # Test single asset (simulate WETH price drop to $1800)
    python manage.py test_liquidation_detection \\
        --asset 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2 \\
        --price 1800

    # Test multiple assets simultaneously
    python manage.py test_liquidation_detection \\
        --asset 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2 \\
        --price 1800 \\
        --asset 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 \\
        --price 0.95

TESTING STRATEGY:

    1. Start by listing assets to see what's available
    2. Pick an asset and simulate a significant price change (e.g., -10% to -20%)
    3. Run the command - it will update DB and run full detection
    4. Monitor logs for [LIQUIDATION_DETECTED] messages
    5. Verify data is written to LiquidationDetections table
    6. Check SimplePush notifications arrive

IMPORTANT:
    - This command writes REAL data to ClickHouse tables
    - Predicted prices are permanently updated in TransactionRawNumerator
    - All detected liquidations are stored in LiquidationDetections
    - Use realistic price changes to get meaningful results

COMMON ASSETS:
    WETH:  0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2  (try ~$1800)
    USDC:  0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48  (try ~$0.95)
    USDT:  0xdAC17F958D2ee523a2206206994597C13D831ec7  (try ~$0.95)
    WBTC:  0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599  (try ~$25000)
"""

import logging
from datetime import datetime

from django.core.management.base import BaseCommand

from oracles.tasks import InsertTransactionNumeratorTask
from utils.clickhouse.client import clickhouse_client

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = (
        "Test liquidation detection by simulating price updates. "
        "Inserts test prices into TransactionRawNumerator and triggers the detection pipeline. "
        "Use --list-assets to see available assets, or specify --asset and --price to test scenarios."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--asset",
            action="append",
            dest="assets",
            help="Asset address to update (can be specified multiple times)",
        )
        parser.add_argument(
            "--price",
            action="append",
            dest="prices",
            type=float,
            help="Predicted price in USD (must match with --asset order)",
        )
        parser.add_argument(
            "--list-assets",
            action="store_true",
            help="List all assets with their current prices",
        )

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("=" * 80))
        self.stdout.write(self.style.SUCCESS("Liquidation Detection Test Tool"))
        self.stdout.write(self.style.SUCCESS("=" * 80))

        # List assets mode
        if options["list_assets"]:
            self._list_assets()
            return

        # Validate inputs
        if not options["assets"] or not options["prices"]:
            self.stdout.write(
                self.style.ERROR("❌ Error: Must specify --asset and --price")
            )
            return

        if len(options["assets"]) != len(options["prices"]):
            self.stdout.write(
                self.style.ERROR(
                    "❌ Error: Number of --asset and --price arguments must match"
                )
            )
            return

        try:
            # Prepare asset/price pairs
            asset_price_pairs = list(zip(options["assets"], options["prices"]))

            # Display what we're about to do
            self._show_planned_updates(asset_price_pairs)

            self._show_current_state()

            # Trigger detection task
            self.stdout.write("\n" + "=" * 80)
            self.stdout.write(
                self.style.SUCCESS("🚀 Triggering Liquidation Detection Task...")
            )
            self.stdout.write("=" * 80 + "\n")

            backup_data = self._trigger_detection(asset_price_pairs)

            # Step 4: Show results
            self.stdout.write("\n" + "=" * 80)
            self.stdout.write(self.style.SUCCESS("📊 Results"))
            self.stdout.write("=" * 80 + "\n")

            self._show_results()

            # Cleanup test data and restore original prices
            self._cleanup_test_data(backup_data)

            self.stdout.write("\n" + "=" * 80)
            self.stdout.write(self.style.SUCCESS("✅ Test Completed Successfully!"))
            self.stdout.write("=" * 80)
            self.stdout.write("\n📝 What happened:")
            self.stdout.write(
                "   1. Original prices were backed up from PriceLatestTransactionRawNumerator"
            )
            self.stdout.write(
                "   2. InsertTransactionNumeratorTask inserted test prices into TransactionRawNumerator"
            )
            self.stdout.write(
                "   3. EstimateFutureLiquidationCandidatesTask calculated health factors with new prices"
            )
            self.stdout.write(
                "   4. At-risk users were identified and stored in LiquidationDetections"
            )
            self.stdout.write(
                "   5. SimplePush notification was sent (if liquidations were detected)"
            )
            self.stdout.write(
                "   6. Original prices were restored to PriceLatestTransactionRawNumerator"
            )
            self.stdout.write("\n🔍 Verify results:")
            self.stdout.write(
                '   clickhouse-client --query "SELECT * FROM aave_ethereum.LiquidationDetections ORDER BY detected_at DESC LIMIT 5 FORMAT Vertical"'
            )
            self.stdout.write("\n📊 Check logs:")
            self.stdout.write('   grep "LIQUIDATION" /path/to/celery.log\n')

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"\n❌ Error: {e}"))
            import traceback

            traceback.print_exc()

    def _show_planned_updates(self, asset_price_pairs):
        """
        Display the planned price updates before execution.

        Shows current prices vs planned test prices for each asset.

        Args:
            asset_price_pairs (list): List of tuples (asset, price_usd)
        """
        self.stdout.write("\n" + "=" * 80)
        self.stdout.write(self.style.SUCCESS("📝 Planned Price Updates"))
        self.stdout.write("=" * 80 + "\n")

        for asset, price in asset_price_pairs:
            asset_info = self._get_asset_info(asset)
            if asset_info:
                symbol = asset_info["symbol"]
                current_predicted = asset_info["predicted_transaction_price"]
                current_historical = asset_info["historical_event_price"]

                self.stdout.write(f"  Asset: {symbol} ({asset[:10]}...)")
                self.stdout.write(
                    f"    Current Historical Price: {current_historical:.2f}"
                )
                self.stdout.write(
                    f"    Current Predicted Price:  {current_predicted:.2f}"
                )
                self.stdout.write(f"    New Test Price:           {price:.2f}")
            else:
                self.stdout.write(self.style.ERROR(f"  ❌ Asset not found: {asset}"))

    def _get_asset_info(self, asset):
        """
        Get asset information from ClickHouse.

        Args:
            asset (str): Asset address (e.g., 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2)

        Returns:
            dict: Asset info containing symbol, decimals_places, historical_event_price,
                  predicted_transaction_price, asset_source, and name, or None if not found
        """
        query = f"""
        SELECT
            lac.symbol,
            lac.decimals_places,
            lac.historical_event_price,
            lac.predicted_transaction_price,
            lasu.source AS asset_source,
            lac.name
        FROM aave_ethereum.view_LatestAssetConfiguration AS lac
        LEFT JOIN aave_ethereum.LatestAssetSourceUpdated AS lasu FINAL
            ON lac.asset = lasu.asset
        WHERE lac.asset = '{asset}'
        """

        result = clickhouse_client.execute_query(query)
        if result.result_rows:
            row = result.result_rows[0]
            return {
                "symbol": row[0],
                "decimals_places": int(row[1]),
                "historical_event_price": float(row[2]),
                "predicted_transaction_price": int(row[3]),
                "asset_source": row[4]
                if row[4]
                else "0x0000000000000000000000000000000000000000",
                "name": row[5] if row[5] else "Unknown",
            }
        return None

    def _show_current_state(self):
        """
        Show current system state before triggering detection.

        Displays:
        - Users with health factors < 2.0 (potentially at risk)
        - Number of existing liquidation candidates in memory table

        This helps establish a baseline to compare against after detection runs.
        """
        self.stdout.write("\n" + "=" * 80)
        self.stdout.write(self.style.SUCCESS("📈 Current System State"))
        self.stdout.write("=" * 80 + "\n")

        # Show users with low health factors
        query = """
        SELECT
            user,
            health_factor,
            effective_collateral_usd,
            effective_debt_usd
        FROM aave_ethereum.view_user_health_factor
        WHERE health_factor between 1.00 AND 1.02
        ORDER BY health_factor ASC
        LIMIT 5
        """

        result = clickhouse_client.execute_query(query)

        if result.result_rows:
            self.stdout.write("Users with low health factors (< 1.02):")
            for row in result.result_rows:
                user, hf, collateral, debt = row
                self.stdout.write(
                    f"  {user[:10]}... | HF: {hf:.4f} | "
                    f"Collateral: ${collateral:,.2f} | Debt: ${debt:,.2f}"
                )
        else:
            self.stdout.write("No users with health factor < 1.02")

        # Show existing liquidation candidates
        query2 = """
        SELECT count()
        FROM aave_ethereum.LiquidationCandidates_Memory
        """

        result2 = clickhouse_client.execute_query(query2)
        count = result2.result_rows[0][0] if result2.result_rows else 0
        self.stdout.write(f"\nCurrent liquidation candidates in memory: {count}")

    def _trigger_detection(self, asset_price_pairs):
        """
        Trigger the liquidation detection task synchronously.

        Creates parsed_numerator_logs in the format expected by
        InsertTransactionNumeratorTask which will insert data into
        TransactionRawNumerator and trigger EstimateFutureLiquidationCandidatesTask.

        The task performs the full detection workflow:
        1. Backs up current prices from PriceLatestTransactionRawNumerator
        2. Inserts test price data into TransactionRawNumerator
        3. Extracts updated asset addresses from the logs
        4. Calculates health factors using predicted prices for updated assets
           and historical prices for all other assets
        5. Compares with current health factors from view_user_health_factor (146)
        6. Identifies users with current HF > 1.0 but predicted HF < 1.0
        7. Retrieves liquidation opportunities from LiquidationCandidates_Memory
        8. Stores all detections in LiquidationDetections log table
        9. Sends SimplePush notification with summary

        Args:
            asset_price_pairs (list): List of tuples (asset, price_usd) for price updates

        Returns:
            list: Backup of original price data for restoration
        """

        # Use microsecond timestamp (Unix timestamp * 1,000,000)
        current_timestamp = int(datetime.now().timestamp() * 1_000_000)

        parsed_numerator_logs = []
        assets_to_backup = []

        for asset, price in asset_price_pairs:
            asset_info = self._get_asset_info(asset)
            if asset_info:
                assets_to_backup.append(asset)
                parsed_numerator_logs.append(
                    [
                        asset,
                        asset_info["asset_source"],
                        asset_info["name"],
                        current_timestamp,
                        999999999,
                        "0xtest_transaction_hash",
                        "transaction",
                        int(price),
                    ]
                )

        if not parsed_numerator_logs:
            self.stdout.write(self.style.ERROR("❌ No valid assets to process"))
            return []

        # Step 1: Backup current prices before inserting test data
        self.stdout.write("📦 Backing up current prices...")
        backup_data = self._backup_current_prices(assets_to_backup)
        self.stdout.write(f"✓ Backed up {len(backup_data)} price records\n")

        # Step 2: Run the task synchronously (not async)
        self.stdout.write(
            f"Triggering detection for {len(parsed_numerator_logs)} assets..."
        )
        self.stdout.write(
            "Running InsertTransactionNumeratorTask (includes insertion + detection)...\n"
        )

        InsertTransactionNumeratorTask.run(
            parsed_numerator_logs=parsed_numerator_logs, hash="0xtest_transaction_hash"
        )
        self.stdout.write(
            self.style.SUCCESS("\n✓ Detection task completed successfully")
        )
        self.stdout.write("All data has been written to ClickHouse tables")

        return backup_data

    def _show_results(self):
        """
        Show the results of the detection run.

        Queries the LiquidationDetections table for entries from the last 5 minutes
        and displays:
        - Total number of liquidation opportunities detected
        - Number of unique users at risk
        - Total potential profit
        - Details of individual detections (user, health factors, profit)

        Note: MEV submission results would be shown here if execution branch is merged.
        """
        # Check LiquidationDetections table
        query = """
        SELECT
            count() as total_detections,
            count(DISTINCT user) as unique_users,
            sum(profit) as total_profit
        FROM aave_ethereum.LiquidationDetections
        WHERE detected_at > now() - INTERVAL 5 MINUTE
        """

        result = clickhouse_client.execute_query(query)
        if result.result_rows:
            total, users, profit = result.result_rows[0]
            self.stdout.write("Liquidations detected in last 5 minutes:")
            self.stdout.write(f"  Total opportunities: {total}")
            self.stdout.write(f"  Unique users: {users}")
            self.stdout.write(
                f"  Total profit: ${profit:,.2f}" if profit else "  Total profit: $0.00"
            )

        # Check recent detections
        query2 = """
        SELECT
            user,
            current_health_factor,
            predicted_health_factor,
            profit
        FROM aave_ethereum.LiquidationDetections
        WHERE detected_at > now() - INTERVAL 5 MINUTE
        ORDER BY detected_at DESC
        LIMIT 5
        """

        result2 = clickhouse_client.execute_query(query2)
        if result2.result_rows:
            self.stdout.write("\nRecent detections:")
            for row in result2.result_rows:
                user, current_hf, predicted_hf, profit = row
                self.stdout.write(
                    f"  {user[:10]}... | Current HF: {current_hf:.4f} → Predicted HF: {predicted_hf:.4f} | "
                    f"Profit: ${profit:,.2f}"
                )

        # Note: MEV submission tracking is on a different branch
        # This section would check LiquidationSubmissions table if execution branch is merged

    def _backup_current_prices(self, assets):
        """
        Backup current prices from PriceLatestTransactionRawNumerator before test.

        Args:
            assets (list): List of asset addresses to backup

        Returns:
            list: List of tuples containing current price data for each asset
        """
        if not assets:
            return []

        assets_str = ", ".join([f"'{asset}'" for asset in assets])
        query = f"""
        SELECT
            asset,
            asset_source,
            name,
            blockTimestamp,
            blockNumber,
            transactionHash,
            type,
            numerator
        FROM aave_ethereum.PriceLatestTransactionRawNumerator
        WHERE asset IN ({assets_str})
        """

        result = clickhouse_client.execute_query(query)
        return result.result_rows if result.result_rows else []

    def _cleanup_test_data(self, backup_data):
        """
        Restore original prices by re-inserting backup data.

        Since PriceLatestTransactionRawNumerator uses ReplacingMergeTree,
        we can't simply delete the test data. Instead, we re-insert the
        original backed-up data with a current timestamp to make it the "latest"
        version, effectively replacing the test data.

        Args:
            backup_data (list): List of tuples containing original price data
        """
        try:
            self.stdout.write("\n" + "=" * 80)
            self.stdout.write(self.style.SUCCESS("🧹 Restoring Original Prices"))
            self.stdout.write("=" * 80 + "\n")

            if not backup_data:
                self.stdout.write("⚠️  No backup data found - skipping restoration")
                return

            import time

            # Use current microsecond timestamp to make restored data "latest"
            current_timestamp = int(time.time() * 1_000_000)

            # Prepare rows for re-insertion with updated timestamp
            restore_rows = []
            for row in backup_data:
                # Convert tuple to list and update blockTimestamp to current time
                restore_row = list(row)
                restore_row[3] = current_timestamp  # Update blockTimestamp
                restore_rows.append(restore_row)

            # Re-insert original data with current timestamp
            self.stdout.write(
                f"📥 Re-inserting {len(restore_rows)} original price records..."
            )
            clickhouse_client.insert_rows(
                "PriceLatestTransactionRawNumerator", restore_rows
            )
            self.stdout.write("✓ Original prices re-inserted with current timestamp")

            # Optimize the table to apply the replacement
            self.stdout.write("🔧 Optimizing table to apply changes...")
            optimize_query = """
            OPTIMIZE TABLE aave_ethereum.PriceLatestTransactionRawNumerator FINAL
            """
            clickhouse_client.execute_query(optimize_query)
            self.stdout.write("✓ Optimized PriceLatestTransactionRawNumerator table")

            self.stdout.write("\n✓ Original prices restored successfully")
            self.stdout.write(
                "   (Test data with older timestamp will be ignored by FINAL queries)"
            )

        except Exception as e:
            self.stdout.write(
                self.style.WARNING(f"\n⚠️  Warning: Restoration failed: {e}")
            )
            self.stdout.write(
                "   The test data may still be affecting predicted prices."
            )
            self.stdout.write("   You may need to manually restore with:")
            self.stdout.write(
                "   1. Query current state: clickhouse-client --query \"SELECT * FROM aave_ethereum.PriceLatestTransactionRawNumerator FINAL WHERE asset = '<asset_address>' FORMAT Vertical\""
            )
            self.stdout.write("   2. Re-insert original data with current timestamp")
            self.stdout.write(
                '   3. Run: clickhouse-client --query "OPTIMIZE TABLE aave_ethereum.PriceLatestTransactionRawNumerator FINAL"'
            )

    def _list_assets(self):
        """
        List all assets available in the system with their current prices.

        Displays a formatted table showing:
        - Asset symbol (e.g., WETH, USDC)
        - Asset address
        - Current historical_event_price (USD)
        - Current predicted_transaction_price (USD)

        This is useful for:
        - Discovering which assets are available for testing
        - Seeing current price ranges to determine realistic test values
        - Getting asset addresses to use with --asset parameter
        """
        self.stdout.write("\n" + "=" * 80)
        self.stdout.write(self.style.SUCCESS("📋 Available Assets"))
        self.stdout.write("=" * 80 + "\n")

        query = """
        SELECT
            asset,
            symbol,
            decimals_places,
            historical_event_price,
            predicted_transaction_price,
            predicted_transaction_price_usd
        FROM aave_ethereum.view_LatestAssetConfiguration
        ORDER BY symbol
        """

        result = clickhouse_client.execute_query(query)

        if result.result_rows:
            self.stdout.write(
                f"{'Symbol':<8} {'Asset':<44} {'Historical $':<15} {'Predicted $':<15}"
            )
            self.stdout.write("-" * 80)

            for row in result.result_rows:
                asset, symbol, decimals, hist_price, pred_price_raw, pred_price_usd = (
                    row
                )

                # Calculate predicted price in USD if not available
                if pred_price_usd == 0 and pred_price_raw > 0:
                    pred_price_usd = pred_price_raw / decimals

                self.stdout.write(
                    f"{symbol:<8} {asset:<44} ${hist_price:<14.2f} ${pred_price_usd:<14.2f}"
                )

            self.stdout.write("\nUsage example:")
            self.stdout.write(
                f"  python manage.py test_liquidation_detection --asset {result.result_rows[0][0]} --price 2000"
            )
        else:
            self.stdout.write("No assets found in the system")
