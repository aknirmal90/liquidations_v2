# Generated by Django 5.1.2 on 2024-12-29 08:53

import django.db.models.deletion
from decimal import Decimal
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [("blockchains", "0007_event_contract_addresses")]

    operations = [
        migrations.CreateModel(
            name="Asset",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("address", models.CharField(max_length=255)),
                ("symbol", models.CharField(max_length=255)),
                (
                    "decimals",
                    models.DecimalField(
                        decimal_places=0, default=Decimal("0"), max_digits=72
                    ),
                ),
                ("is_enabled", models.BooleanField(default=False)),
                (
                    "liquidation_threshold",
                    models.DecimalField(
                        blank=True, decimal_places=6, max_digits=12, null=True
                    ),
                ),
                (
                    "liquidation_bonus",
                    models.DecimalField(
                        blank=True, decimal_places=6, max_digits=12, null=True
                    ),
                ),
                (
                    "emode_liquidation_threshold",
                    models.DecimalField(
                        blank=True, decimal_places=6, max_digits=12, null=True
                    ),
                ),
                (
                    "emode_liquidation_bonus",
                    models.DecimalField(
                        blank=True, decimal_places=6, max_digits=12, null=True
                    ),
                ),
                (
                    "pricesource",
                    models.CharField(blank=True, max_length=255, null=True),
                ),
                ("contractA", models.CharField(blank=True, max_length=255, null=True)),
                ("contractB", models.CharField(blank=True, max_length=255, null=True)),
                (
                    "priceA",
                    models.DecimalField(
                        blank=True,
                        decimal_places=0,
                        default=Decimal("1.0"),
                        max_digits=72,
                        null=True,
                    ),
                ),
                (
                    "priceB",
                    models.DecimalField(
                        blank=True,
                        decimal_places=0,
                        default=Decimal("1.0"),
                        max_digits=72,
                        null=True,
                    ),
                ),
                (
                    "numerator",
                    models.DecimalField(
                        decimal_places=0, default=Decimal("1"), max_digits=36
                    ),
                ),
                (
                    "denominator",
                    models.DecimalField(
                        decimal_places=0, default=Decimal("1"), max_digits=36
                    ),
                ),
                (
                    "price",
                    models.DecimalField(
                        blank=True,
                        decimal_places=36,
                        default=Decimal("1.0"),
                        max_digits=72,
                        null=True,
                    ),
                ),
                (
                    "price_in_usdt",
                    models.DecimalField(
                        blank=True,
                        decimal_places=36,
                        default=Decimal("1.0"),
                        max_digits=72,
                        null=True,
                    ),
                ),
                ("updated_at_block_heightA", models.PositiveIntegerField(default=0)),
                ("updated_at_block_heightB", models.PositiveIntegerField(default=0)),
                ("emode_category", models.PositiveSmallIntegerField(default=0)),
                (
                    "network",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        to="blockchains.network",
                    ),
                ),
                (
                    "protocol",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        to="blockchains.protocol",
                    ),
                ),
            ],
            options={"unique_together": {("network", "protocol", "address")}},
        )
    ]
