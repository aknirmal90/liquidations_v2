# Generated by Django 5.1.2 on 2025-01-06 18:46

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [("aave", "0048_alter_aaveburnevent_unique_together_and_more")]

    operations = [
        migrations.AddField(
            model_name="aavebalancelog",
            name="collateral_is_enabled_updated_at_block",
            field=models.PositiveBigIntegerField(default=0),
        )
    ]
