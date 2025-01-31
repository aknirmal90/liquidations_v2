# Generated by Django 5.1.2 on 2024-12-31 06:31

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("aave", "0014_rename_epoch_and_rounda_asset_updated_at_db_a_and_more"),
        ("blockchains", "0010_alter_network_wss"),
    ]

    operations = [
        migrations.CreateModel(
            name="AssetPriceLog",
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
                ("asset_address", models.CharField(max_length=42)),
                (
                    "price",
                    models.DecimalField(
                        blank=True, decimal_places=36, max_digits=72, null=True
                    ),
                ),
                ("onchain_created_at", models.DateTimeField()),
                ("db_created_at", models.DateTimeField(auto_now_add=True)),
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
            options={
                "indexes": [
                    models.Index(
                        fields=["asset_address", "protocol", "network"],
                        name="aave_assetp_asset_a_9e966b_idx",
                    )
                ]
            },
        )
    ]
