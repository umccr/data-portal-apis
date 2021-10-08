# Generated by Django 3.2.7 on 2021-10-06 09:24

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_portal', '0033_auto_20210928_0456'),
    ]

    operations = [
        migrations.CreateModel(
            name='Sequence',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('instrument_run_id', models.CharField(max_length=255)),
                ('run_id', models.CharField(max_length=255)),
                ('sample_sheet_name', models.CharField(max_length=255)),
                ('gds_folder_path', models.CharField(max_length=255)),
                ('gds_volume_name', models.CharField(max_length=255)),
                ('reagent_barcode', models.CharField(max_length=255)),
                ('flowcell_barcode', models.CharField(max_length=255)),
                ('status', models.CharField(choices=[('started', 'Started'), ('failed', 'Failed'), ('succeeded', 'Succeeded')], max_length=255)),
                ('start_time', models.DateTimeField()),
                ('end_time', models.DateTimeField(blank=True, null=True)),
            ],
            options={
                'unique_together': {('instrument_run_id', 'run_id')},
            },
        ),
    ]