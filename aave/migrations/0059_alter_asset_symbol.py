# Generated by Django 5.1.2 on 2025-01-19 06:55

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('aave', '0058_alter_asset_num_decimals'),
    ]

    operations = [
        migrations.AlterField(
            model_name='asset',
            name='symbol',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]
