# Generated by Django 3.2.7 on 2021-10-28 04:34

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('data_portal', '0036_libraryrun_workflows'),
    ]

    operations = [
        migrations.DeleteModel(
            name='Configuration',
        ),
        migrations.DeleteModel(
            name='S3LIMS',
        ),
    ]
