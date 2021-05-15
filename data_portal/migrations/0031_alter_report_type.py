# Generated by Django 3.2 on 2021-05-15 04:01

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_portal', '0030_alter_report_type'),
    ]

    operations = [
        migrations.AlterField(
            model_name='report',
            name='type',
            field=models.CharField(choices=[('cttso500', 'Cttso500'), ('cttso500_msi', 'Cttso500 Msi'), ('cttso500_tmb', 'Cttso500 Tmb'), ('cttso500_fusion_caller_metrics', 'Cttso500 Fusion Caller Metrics'), ('cttso500_failed_exon_coverage_qc', 'Cttso500 Failed Exon Coverage Qc'), ('cttso500_sample_analysis_results', 'Cttso500 Sample Analysis Results'), ('cttso500_target_region_coverage', 'Cttso500 Target Region Coverage'), ('qc_summary', 'Qc Summary'), ('multiqc', 'Multiqc'), ('report_inputs', 'Report Inputs'), ('hrd_chord', 'Hrd Chord'), ('hrd_hrdetect', 'Hrd Hrdetect'), ('purple_cnv_germ', 'Purple Cnv Germ'), ('purple_cnv_som', 'Purple Cnv Som'), ('purple_cnv_som_gene', 'Purple Cnv Som Gene'), ('sigs_dbs', 'Sigs Dbs'), ('sigs_indel', 'Sigs Indel'), ('sigs_snv_2015', 'Sigs Snv 2015'), ('sigs_snv_2020', 'Sigs Snv 2020'), ('sv_unmelted', 'Sv Unmelted'), ('sv_melted', 'Sv Melted'), ('sv_bnd_main', 'Sv Bnd Main'), ('sv_bnd_purpleinf', 'Sv Bnd Purpleinf'), ('sv_nobnd_main', 'Sv Nobnd Main'), ('sv_nobnd_other', 'Sv Nobnd Other'), ('sv_nobnd_manygenes', 'Sv Nobnd Manygenes'), ('sv_nobnd_manytranscripts', 'Sv Nobnd Manytranscripts')], max_length=255),
        ),
    ]
