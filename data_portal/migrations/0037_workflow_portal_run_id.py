# Generated by Django 3.2.7 on 2021-11-11 04:48

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_portal', '0036_libraryrun_workflows'),
    ]

    operations = [
        migrations.AddField(
            model_name='workflow',
            name='portal_run_id',
            field=models.CharField(max_length=255, null=True),
        ),
    ]
