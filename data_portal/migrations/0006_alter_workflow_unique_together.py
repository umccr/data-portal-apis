# Generated by Django 4.2.3 on 2023-09-05 09:20

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('data_portal', '0005_backfill_portal_run_id'),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name='workflow',
            unique_together=set(),
        ),
    ]
