# Generated by Django 5.1.2 on 2024-12-29 13:21

from decimal import Decimal
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [("aave", "0004_rename_address_asset_asset_and_more")]

    operations = [
        migrations.AlterField(
            model_name="asset",
            name="decimals",
            field=models.DecimalField(
                decimal_places=0, default=Decimal("0"), max_digits=72
            ),
        ),
        migrations.AlterField(
            model_name="asset",
            name="num_decimals",
            field=models.DecimalField(
                decimal_places=0, default=Decimal("1"), max_digits=3
            ),
        ),
    ]