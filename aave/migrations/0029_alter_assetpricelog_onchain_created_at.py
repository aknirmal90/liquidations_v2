# Generated by Django 5.1.2 on 2025-01-02 09:40

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [("aave", "0028_remove_asset_updated_at_block_heighta_and_more")]

    operations = [
        migrations.AlterField(
            model_name="assetpricelog",
            name="onchain_created_at",
            field=models.DateTimeField(blank=True, null=True),
        )
    ]
