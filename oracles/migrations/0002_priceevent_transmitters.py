# Generated by Django 5.1.2 on 2025-06-28 19:38

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("oracles", "0001_initial")]

    operations = [
        migrations.AddField(
            model_name="priceevent",
            name="transmitters",
            field=models.JSONField(blank=True, null=True),
        )
    ]
