# Generated by Django 5.1.2 on 2025-01-05 14:53

from decimal import Decimal
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("aave", "0039_remove_asset_interest_rate_strategy_address_and_more")
    ]

    operations = [
        migrations.AddField(
            model_name="aavebalancelog",
            name="mark_for_deletion",
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name="asset",
            name="liquidation_bonus",
            field=models.DecimalField(
                decimal_places=6, default=Decimal("0"), max_digits=12
            ),
        ),
        migrations.AlterField(
            model_name="asset",
            name="liquidation_threshold",
            field=models.DecimalField(
                decimal_places=6, default=Decimal("0"), max_digits=12
            ),
        ),
    ]
