# Generated by Django 5.1.2 on 2024-12-29 10:43

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [("blockchains", "0007_event_contract_addresses")]

    operations = [migrations.DeleteModel(name="Token")]