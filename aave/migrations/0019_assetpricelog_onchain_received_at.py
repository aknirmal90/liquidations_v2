# Generated by Django 5.1.2 on 2024-12-31 10:24

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [("aave", "0018_assetpricelog_round_id")]

    operations = [
        migrations.AddField(
            model_name="assetpricelog",
            name="onchain_received_at",
            field=models.DateTimeField(blank=True, null=True),
        )
    ]
