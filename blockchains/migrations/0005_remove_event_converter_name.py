# Generated by Django 5.1.2 on 2024-12-29 08:04

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [("blockchains", "0004_rename_event_abi_event_abi_and_more")]

    operations = [migrations.RemoveField(model_name="event", name="converter_name")]
