# Generated by Django 5.1.2 on 2024-12-29 20:32

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("aave", "0011_asset_reserve_factor_asset_reserve_is_borrow_enabled_and_more")
    ]

    operations = [
        migrations.AddField(
            model_name="asset",
            name="emode_is_borrowable",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="asset",
            name="emode_is_collateral",
            field=models.BooleanField(default=False),
        ),
    ]
