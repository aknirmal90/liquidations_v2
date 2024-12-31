# Generated by Django 5.1.2 on 2024-12-29 09:12

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [("aave", "0001_initial")]

    operations = [
        migrations.AddField(
            model_name="asset",
            name="atoken_address",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name="asset",
            name="interest_rate_strategy_address",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name="asset",
            name="stable_debt_token_address",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name="asset",
            name="variable_debt_token_address",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]