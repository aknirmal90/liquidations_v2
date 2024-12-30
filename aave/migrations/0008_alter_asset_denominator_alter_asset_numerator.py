# Generated by Django 5.1.2 on 2024-12-29 18:30

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("aave", "0007_alter_asset_denominator_alter_asset_numerator_and_more")
    ]

    operations = [
        migrations.AlterField(
            model_name="asset",
            name="denominator",
            field=models.DecimalField(
                blank=True, decimal_places=0, max_digits=36, null=True
            ),
        ),
        migrations.AlterField(
            model_name="asset",
            name="numerator",
            field=models.DecimalField(
                blank=True, decimal_places=0, max_digits=36, null=True
            ),
        ),
    ]
