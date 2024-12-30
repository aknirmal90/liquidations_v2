# Generated by Django 5.1.2 on 2024-12-14 15:10

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [("blockchains", "0002_remove_protocol_type")]

    operations = [
        migrations.AddField(
            model_name="event",
            name="converter_name",
            field=models.CharField(max_length=256, null=True),
        ),
        migrations.AddField(
            model_name="event",
            name="model_class",
            field=models.CharField(max_length=256, null=True),
        ),
        migrations.AddField(
            model_name="event",
            name="model_primary_key",
            field=models.CharField(max_length=256, null=True),
        ),
    ]
