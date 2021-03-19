# Generated by Django 3.1.1 on 2021-03-19 00:57

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_portal', '0020_report'),
    ]

    operations = [
        migrations.AlterField(
            model_name='report',
            name='type',
            field=models.CharField(choices=[('hrd_chord', 'Hrd Chord'), ('hrd_hrdetect', 'Hrd Hrdetect'), ('purple_cnv_germ', 'Purple Cnv Germ'), ('purple_cnv_som', 'Purple Cnv Som'), ('purple_cnv_som_gene', 'Purple Cnv Som Gene'), ('sigs_dbs', 'Sigs Dbs'), ('sigs_indel', 'Sigs Indel'), ('sigs_snv_2015', 'Sigs Snv 2015'), ('sigs_snv_2020', 'Sigs Snv 2020'), ('sv_unmelted', 'Sv Unmelted'), ('sv_melted', 'Sv Melted'), ('sv_bnd_main', 'Sv Bnd Main'), ('sv_bnd_purpleinf', 'Sv Bnd Purpleinf'), ('sv_nobnd_main', 'Sv Nobnd Main'), ('sv_nobnd_other', 'Sv Nobnd Other'), ('sv_nobnd_manygenes', 'Sv Nobnd Manygenes'), ('sv_nobnd_manytranscripts', 'Sv Nobnd Manytranscripts')], max_length=255),
        ),
    ]
