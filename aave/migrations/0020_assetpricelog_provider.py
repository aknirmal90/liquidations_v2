# Generated by Django 5.1.2 on 2024-12-31 11:44

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [("aave", "0019_assetpricelog_onchain_received_at")]

    operations = [
        migrations.AddField(
            model_name="assetpricelog",
            name="provider",
            field=models.CharField(blank=True, max_length=255, null=True),
        )
    ]
