# Generated by Django 5.1.2 on 2025-01-01 04:29

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [("aave", "0021_rename_numerator_asset_decimals_price_and_more")]

    operations = [
        migrations.AlterField(
            model_name="asset",
            name="decimals_price",
            field=models.DecimalField(
                blank=True, decimal_places=0, max_digits=72, null=True
            ),
        ),
        migrations.AlterField(
            model_name="asset",
            name="max_cap",
            field=models.DecimalField(
                blank=True, decimal_places=0, max_digits=72, null=True
            ),
        ),
    ]
