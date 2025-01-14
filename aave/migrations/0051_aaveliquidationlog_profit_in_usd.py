# Generated by Django 5.1.2 on 2025-01-14 14:16

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [("aave", "0050_aaveliquidationlog_block_datetime")]

    operations = [
        migrations.AddField(
            model_name="aaveliquidationlog",
            name="profit_in_usd",
            field=models.DecimalField(
                blank=True, decimal_places=2, max_digits=70, null=True
            ),
        )
    ]
