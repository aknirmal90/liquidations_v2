# Generated by Django 5.1.2 on 2024-12-31 11:34

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [("blockchains", "0010_alter_network_wss")]

    operations = [
        migrations.RenameField(
            model_name="network", old_name="wss", new_name="wss_infura"
        ),
        migrations.AddField(
            model_name="network",
            name="wss_alchemy",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name="network",
            name="wss_nodereal",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name="network",
            name="wss_quicknode",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]