# Generated by Django 5.1.2 on 2024-12-31 13:01

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        (
            "blockchains",
            "0011_rename_wss_network_wss_infura_network_wss_alchemy_and_more",
        )
    ]

    operations = [
        migrations.AddField(
            model_name="network",
            name="wss_tatum",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name="network",
            name="wss_tenderly",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]