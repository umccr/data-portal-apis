# Generated by Django 2.1.11 on 2019-10-01 06:25

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('data_portal', '0005_auto_20190926_0254'),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name='limsrow',
            unique_together={('illumina_id', 'sample_id')},
        ),
    ]