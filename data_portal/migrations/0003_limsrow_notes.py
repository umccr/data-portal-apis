# Generated by Django 2.1.4 on 2019-08-29 01:16

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_portal', '0002_auto_20190829_0112'),
    ]

    operations = [
        migrations.AddField(
            model_name='limsrow',
            name='notes',
            field=models.CharField(default=None, max_length=255),
            preserve_default=False,
        ),
    ]
