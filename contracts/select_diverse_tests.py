#!/usr/bin/env python3
"""
Select diverse tests from samples.csv
Picks 2 liquidations for each unique debt/collateral asset pair
"""

import csv
import sys
from collections import defaultdict


def select_diverse_tests(csv_path, tests_per_pair=2):
    """Select tests with diverse debt/collateral pairs"""

    # Group liquidations by (debt_asset, collateral_asset) pair
    pairs = defaultdict(list)

    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, 1):
            debt = row["debt_asset"].lower()
            collateral = row["collateral_asset"].lower()
            pair = (debt, collateral)
            pairs[pair].append((i, row))

    # Select up to tests_per_pair liquidations from each pair
    selected = []
    for pair, liquidations in sorted(pairs.items()):
        # Take first tests_per_pair liquidations from this pair
        for idx, (original_idx, row) in enumerate(liquidations[:tests_per_pair]):
            selected.append((original_idx, row, pair))
            print(
                f"Selected liquidation #{original_idx} - Debt: {pair[0][:10]}..., Collateral: {pair[1][:10]}...",
                file=sys.stderr,
            )

    print(f"\nTotal pairs: {len(pairs)}", file=sys.stderr)
    print(f"Total tests selected: {len(selected)}", file=sys.stderr)

    # Print summary by pair
    print("\nSummary by pair:", file=sys.stderr)
    for pair in sorted(pairs.keys()):
        count = min(tests_per_pair, len(pairs[pair]))
        print(f"  {pair[0][-4:]} / {pair[1][-4:]}: {count} tests", file=sys.stderr)

    return selected


if __name__ == "__main__":
    csv_path = "samples.csv"
    tests_per_pair = 2 if len(sys.argv) < 2 else int(sys.argv[1])

    selected = select_diverse_tests(csv_path, tests_per_pair)

    # Print indices for use with generate_tests.py
    indices = [idx for idx, _, _ in selected]
    print("\n".join(map(str, indices)))
