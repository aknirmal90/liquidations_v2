# Generated by Django 5.1.2 on 2024-12-29 12:10

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("aave", "0003_asset_num_decimals_alter_asset_decimals"),
        ("blockchains", "0008_delete_token"),
    ]

    operations = [
        migrations.RenameField(
            model_name="asset", old_name="address", new_name="asset"
        ),
        migrations.AlterUniqueTogether(
            name="asset", unique_together={("network", "protocol", "asset")}
        ),
    ]