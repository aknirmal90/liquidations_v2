# Generated by Django 5.1.2 on 2025-01-13 20:17

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        (
            "blockchains",
            "0017_rename_wss_sequencer_virginia_network_wss_sequencer_oregon",
        )
    ]

    operations = [
        migrations.CreateModel(
            name="ApproximateBlockTimestamp",
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
                ("reference_block_number", models.BigIntegerField(db_index=True)),
                ("timestamp", models.BigIntegerField(blank=True, null=True)),
                (
                    "block_time_in_milliseconds",
                    models.BigIntegerField(blank=True, null=True),
                ),
                (
                    "network",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        to="blockchains.network",
                        unique=True,
                    ),
                ),
            ],
        )
    ]
