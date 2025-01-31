# Generated by Django 5.1.2 on 2025-01-06 08:00

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [("aave", "0040_aavebalancelog_mark_for_deletion_and_more")]

    operations = [
        migrations.AlterField(
            model_name="aaveburnevent",
            name="balance_log",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE, to="aave.aavebalancelog"
            ),
        ),
        migrations.AlterField(
            model_name="aavemintevent",
            name="balance_log",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE, to="aave.aavebalancelog"
            ),
        ),
        migrations.AlterField(
            model_name="aavesupplyevent",
            name="balance_log",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE, to="aave.aavebalancelog"
            ),
        ),
        migrations.AlterField(
            model_name="aavetransferevent",
            name="balance_log",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE, to="aave.aavebalancelog"
            ),
        ),
        migrations.AlterField(
            model_name="aavewithdrawevent",
            name="balance_log",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE, to="aave.aavebalancelog"
            ),
        ),
    ]
