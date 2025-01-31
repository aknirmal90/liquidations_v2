# Generated by Django 5.1.2 on 2025-01-06 12:30

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [("aave", "0042_aavedataqualityanalyticsreport")]

    operations = [
        migrations.RenameField(
            model_name="aavebalancelog",
            old_name="last_updated_liquidity_index",
            new_name="last_updated_collateral_liquidity_index",
        ),
        migrations.RenameField(
            model_name="asset",
            old_name="liquidity_index",
            new_name="collateral_liquidity_index",
        ),
        migrations.AddField(
            model_name="aavebalancelog",
            name="borrow_amount_live",
            field=models.DecimalField(
                blank=True, decimal_places=18, max_digits=72, null=True
            ),
        ),
        migrations.AddField(
            model_name="aavebalancelog",
            name="borrow_amount_live_is_verified",
            field=models.BooleanField(blank=True, default=None, null=True),
        ),
        migrations.AddField(
            model_name="aavebalancelog",
            name="last_updated_borrow_liquidity_index",
            field=models.DecimalField(
                blank=True, decimal_places=0, max_digits=72, null=True
            ),
        ),
        migrations.AddField(
            model_name="asset",
            name="borrow_liquidity_index",
            field=models.DecimalField(
                blank=True, decimal_places=0, max_digits=72, null=True
            ),
        ),
    ]
