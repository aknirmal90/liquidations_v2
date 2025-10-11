# Generated manually for MEV Share transaction log model

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('blockchains', '0002_initial'),
        ('aave', '0064_aavedataqualityanalyticsreport_num_all_unverified_and_more'),
    ]

    operations = [
        migrations.CreateModel(
            name='MevShareTransactionLog',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('transaction_hash', models.CharField(max_length=66, unique=True)),
                ('asset_address', models.CharField(max_length=42)),
                ('price', models.DecimalField(blank=True, decimal_places=0, max_digits=72, null=True)),
                ('round_id', models.PositiveIntegerField(blank=True, null=True)),
                ('block_height', models.PositiveIntegerField(blank=True, null=True)),
                ('mev_received_at', models.DateTimeField()),
                ('onchain_created_at', models.DateTimeField(blank=True, null=True)),
                ('processed_at', models.DateTimeField()),
                ('db_created_at', models.DateTimeField(auto_now_add=True)),
                ('is_mev_opportunity', models.BooleanField(default=False)),
                ('frontrun_detected', models.BooleanField(default=False)),
                ('backrun_detected', models.BooleanField(default=False)),
                ('raw_transaction_data', models.JSONField(blank=True, null=True)),
                ('network', models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='blockchains.network')),
            ],
            options={
                'indexes': [
                    models.Index(fields=['asset_address', 'network'], name='aave_mevsha_asset_a_6a8c73_idx'),
                    models.Index(fields=['transaction_hash'], name='aave_mevsha_transac_c6da73_idx'),
                    models.Index(fields=['mev_received_at'], name='aave_mevsha_mev_rec_4b2c54_idx'),
                    models.Index(fields=['is_mev_opportunity'], name='aave_mevsha_is_mev__3a8f12_idx'),
                ],
            },
        ),
    ]