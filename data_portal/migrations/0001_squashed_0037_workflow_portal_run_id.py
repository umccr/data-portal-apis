# Generated by Django 3.2.9 on 2021-11-26 14:17

import data_portal.fields
from django.db import migrations, models
import django.db.models.deletion
import uuid


class Migration(migrations.Migration):

    replaces = [('data_portal', '0001_initial'), ('data_portal', '0002_auto_20190829_0112'), ('data_portal', '0003_limsrow_notes'), ('data_portal', '0004_auto_20190829_0117'), ('data_portal', '0005_auto_20190926_0254'), ('data_portal', '0006_auto_20191001_0625'), ('data_portal', '0007_auto_20191002_0603'), ('data_portal', '0008_auto_20191015_2337'), ('data_portal', '0009_auto_20191016_0050'), ('data_portal', '0010_auto_20200123_0644'), ('data_portal', '0011_auto_20200123_0649'), ('data_portal', '0012_auto_20200123_0655'), ('data_portal', '0013_auto_20200123_0705'), ('data_portal', '0014_gdsfile'), ('data_portal', '0015_auto_20200416_0528'), ('data_portal', '0016_sequencerun'), ('data_portal', '0017_workflow'), ('data_portal', '0018_auto_20201007_0602'), ('data_portal', '0019_batchrun_notified'), ('data_portal', '0020_report'), ('data_portal', '0021_auto_20210319_0057'), ('data_portal', '0020_fastqlistrow'), ('data_portal', '0022_merge_20210411_0614'), ('data_portal', '0023_auto_20210416_1254'), ('data_portal', '0024_auto_20210422_0634'), ('data_portal', '0025_auto_20210427_0649'), ('data_portal', '0026_auto_20210430_1053'), ('data_portal', '0023_labmetadata'), ('data_portal', '0027_merge_0023_labmetadata_0026_auto_20210430_1053'), ('data_portal', '0028_alter_labmetadata_id'), ('data_portal', '0029_alter_report_type'), ('data_portal', '0030_alter_report_type'), ('data_portal', '0031_alter_report_type'), ('data_portal', '0030_auto_20210621_0440'), ('data_portal', '0032_merge_0030_auto_20210621_0440_0031_alter_report_type'), ('data_portal', '0033_auto_20210928_0456'), ('data_portal', '0034_sequence'), ('data_portal', '0035_libraryrun'), ('data_portal', '0036_libraryrun_workflows'), ('data_portal', '0037_workflow_portal_run_id')]

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='LIMSRow',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('illumina_id', models.CharField(max_length=255)),
                ('run', models.IntegerField()),
                ('timestamp', models.DateField()),
                ('sample_id', models.CharField(max_length=255)),
                ('sample_name', models.CharField(blank=True, max_length=255, null=True)),
                ('subject_id', models.CharField(blank=True, max_length=255, null=True)),
                ('type', models.CharField(blank=True, max_length=255, null=True)),
                ('phenotype', models.CharField(blank=True, max_length=255, null=True)),
                ('source', models.CharField(blank=True, max_length=255, null=True)),
                ('quality', models.CharField(blank=True, max_length=255, null=True)),
                ('secondary_analysis', models.CharField(blank=True, max_length=255, null=True)),
                ('fastq', models.TextField(blank=True, null=True)),
                ('number_fastqs', models.CharField(blank=True, max_length=255, null=True)),
                ('results', models.TextField(blank=True, null=True)),
                ('todo', models.CharField(blank=True, max_length=255, null=True)),
                ('trello', models.TextField(blank=True, null=True)),
                ('notes', models.TextField(blank=True, null=True)),
                ('assay', models.CharField(blank=True, max_length=255, null=True)),
                ('external_library_id', models.CharField(blank=True, max_length=255, null=True)),
                ('external_sample_id', models.CharField(blank=True, max_length=255, null=True)),
                ('external_subject_id', models.CharField(blank=True, max_length=255, null=True)),
                ('library_id', models.CharField(max_length=255)),
                ('project_name', models.CharField(blank=True, max_length=255, null=True)),
                ('project_owner', models.CharField(blank=True, max_length=255, null=True)),
                ('topup', models.CharField(blank=True, max_length=255, null=True)),
                ('override_cycles', models.CharField(blank=True, max_length=255, null=True)),
                ('workflow', models.CharField(blank=True, max_length=255, null=True)),
            ],
            options={
                'unique_together': {('illumina_id', 'library_id')},
            },
        ),
        migrations.CreateModel(
            name='S3Object',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('bucket', models.CharField(max_length=255)),
                ('key', models.TextField()),
                ('size', models.BigIntegerField()),
                ('last_modified_date', models.DateTimeField()),
                ('e_tag', models.CharField(max_length=255)),
                ('unique_hash', data_portal.fields.HashField(base_fields=['bucket', 'key'], default=None, unique=True)),
            ],
            options={
                'unique_together': set(),
            },
        ),
        migrations.CreateModel(
            name='SequenceRun',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('run_id', models.CharField(max_length=255)),
                ('date_modified', models.DateTimeField()),
                ('status', models.CharField(max_length=255)),
                ('gds_folder_path', models.TextField()),
                ('gds_volume_name', models.TextField()),
                ('reagent_barcode', models.CharField(max_length=255)),
                ('v1pre3_id', models.CharField(max_length=255)),
                ('acl', models.TextField()),
                ('flowcell_barcode', models.CharField(max_length=255)),
                ('sample_sheet_name', models.CharField(max_length=255)),
                ('api_url', models.TextField()),
                ('name', models.CharField(max_length=255)),
                ('instrument_run_id', models.CharField(max_length=255)),
                ('msg_attr_action', models.CharField(max_length=255)),
                ('msg_attr_action_type', models.CharField(max_length=255)),
                ('msg_attr_action_date', models.DateTimeField()),
                ('msg_attr_produced_by', models.CharField(max_length=255)),
            ],
            options={
                'unique_together': {('run_id', 'date_modified', 'status')},
            },
        ),
        migrations.CreateModel(
            name='Batch',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(max_length=255)),
                ('created_by', models.CharField(max_length=255)),
                ('context_data', models.TextField(blank=True, null=True)),
            ],
            options={
                'unique_together': {('name', 'created_by')},
            },
        ),
        migrations.CreateModel(
            name='BatchRun',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('step', models.CharField(max_length=255)),
                ('running', models.BooleanField(blank=True, null=True)),
                ('batch', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='data_portal.batch')),
                ('notified', models.BooleanField(blank=True, null=True)),
            ],
        ),
        migrations.CreateModel(
            name='Configuration',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(max_length=255, unique=True)),
                ('value', models.TextField()),
            ],
        ),
        migrations.CreateModel(
            name='FastqListRow',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('rgid', models.CharField(max_length=255)),
                ('rgsm', models.CharField(max_length=255)),
                ('rglb', models.CharField(max_length=255)),
                ('lane', models.IntegerField()),
                ('read_1', models.TextField()),
                ('read_2', models.TextField(blank=True, null=True)),
                ('sequence_run', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='data_portal.sequencerun')),
            ],
            options={
                'unique_together': {('rgid',)},
            },
        ),
        migrations.CreateModel(
            name='GDSFile',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('file_id', models.CharField(max_length=255)),
                ('name', models.TextField()),
                ('volume_id', models.CharField(max_length=255)),
                ('volume_name', models.TextField()),
                ('type', models.CharField(blank=True, max_length=255, null=True)),
                ('tenant_id', models.CharField(max_length=255)),
                ('sub_tenant_id', models.CharField(max_length=255)),
                ('path', models.TextField()),
                ('time_created', models.DateTimeField()),
                ('created_by', models.CharField(max_length=255)),
                ('time_modified', models.DateTimeField()),
                ('modified_by', models.CharField(max_length=255)),
                ('inherited_acl', models.TextField(blank=True, null=True)),
                ('urn', models.TextField()),
                ('size_in_bytes', models.BigIntegerField()),
                ('is_uploaded', models.BooleanField(null=True)),
                ('archive_status', models.CharField(max_length=255)),
                ('time_archived', models.DateTimeField(blank=True, null=True)),
                ('storage_tier', models.CharField(max_length=255)),
                ('presigned_url', models.TextField(blank=True, null=True)),
                ('unique_hash', data_portal.fields.HashField(base_fields=['volume_name', 'path'], default=None, unique=True)),
            ],
        ),
        migrations.CreateModel(
            name='S3LIMS',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('lims_row', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='data_portal.limsrow')),
                ('s3_object', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='data_portal.s3object')),
            ],
            options={
                'unique_together': {('s3_object', 'lims_row')},
            },
        ),
        migrations.CreateModel(
            name='Workflow',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('wfr_name', models.TextField(blank=True, null=True)),
                ('sample_name', models.CharField(blank=True, max_length=255, null=True)),
                ('type_name', models.CharField(max_length=255)),
                ('wfr_id', models.CharField(max_length=255)),
                ('wfl_id', models.CharField(max_length=255)),
                ('wfv_id', models.CharField(max_length=255)),
                ('version', models.CharField(max_length=255)),
                ('input', models.TextField()),
                ('start', models.DateTimeField()),
                ('output', models.TextField(blank=True, null=True)),
                ('end', models.DateTimeField(blank=True, null=True)),
                ('end_status', models.CharField(blank=True, max_length=255, null=True)),
                ('notified', models.BooleanField(blank=True, null=True)),
                ('sequence_run', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='data_portal.sequencerun')),
                ('batch_run', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='data_portal.batchrun')),
                ('portal_run_id', models.CharField(max_length=255, null=True)),
            ],
            options={
                'unique_together': {('wfr_id', 'wfl_id', 'wfv_id')},
            },
        ),
        migrations.CreateModel(
            name='LabMetadata',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('library_id', models.CharField(max_length=255, unique=True)),
                ('sample_name', models.CharField(max_length=255)),
                ('sample_id', models.CharField(max_length=255)),
                ('external_sample_id', models.CharField(blank=True, max_length=255, null=True)),
                ('subject_id', models.CharField(blank=True, max_length=255, null=True)),
                ('external_subject_id', models.CharField(blank=True, max_length=255, null=True)),
                ('phenotype', models.CharField(choices=[('negative-control', 'N Control'), ('normal', 'Normal'), ('tumor', 'Tumor')], max_length=255)),
                ('quality', models.CharField(choices=[('ascites', 'Acites'), ('blood', 'Blood'), ('bone-marrow', 'Bone'), ('buccal', 'Buccal'), ('cell-line', 'Cell Line'), ('cfDNA', 'Cf Dna'), ('cyst-fluid', 'Cyst'), ('DNA', 'Dna'), ('eyebrow-hair', 'Eyebrow'), ('FFPE', 'Ffpe'), ('FNA', 'Fna'), ('OCT', 'Oct'), ('organoid', 'Organoid'), ('PDX-tissue', 'Pdx'), ('plasma-serum', 'Plasma'), ('RNA', 'Rna'), ('tissue', 'Tissue'), ('water', 'Water')], max_length=255)),
                ('source', models.CharField(choices=[('ascites', 'Acites'), ('blood', 'Blood'), ('bone-marrow', 'Bone'), ('buccal', 'Buccal'), ('cell-line', 'Cell Line'), ('cfDNA', 'Cf Dna'), ('cyst-fluid', 'Cyst'), ('DNA', 'Dna'), ('eyebrow-hair', 'Eyebrow'), ('FFPE', 'Ffpe'), ('FNA', 'Fna'), ('OCT', 'Oct'), ('organoid', 'Organoid'), ('PDX-tissue', 'Pdx'), ('plasma-serum', 'Plasma'), ('RNA', 'Rna'), ('tissue', 'Tissue'), ('water', 'Water')], max_length=255)),
                ('project_name', models.CharField(blank=True, max_length=255, null=True)),
                ('project_owner', models.CharField(blank=True, max_length=255, null=True)),
                ('experiment_id', models.CharField(blank=True, max_length=255, null=True)),
                ('type', models.CharField(choices=[('ctDNA', 'Ct Dna'), ('ctTSO', 'Ct Tso'), ('exome', 'Exome'), ('other', 'Other'), ('10X', 'Ten X'), ('TSO-DNA', 'Tso Dna'), ('TSO-RNA', 'Tso Rna'), ('WGS', 'Wgs'), ('WTS', 'Wts')], max_length=255)),
                ('assay', models.CharField(choices=[('AgSsCRE', 'Ag Ss Cre'), ('ctTSO', 'Ct Tso'), ('NebDNA', 'Neb Dna'), ('NebDNAu', 'Neb Dna U'), ('NebRNA', 'Neb Rna'), ('PCR-Free-Tagmentation', 'Pcr Free'), ('10X-3prime-expression', 'Ten X 3Prime'), ('10X-5prime-expression', 'Ten X 5Prime'), ('10X-ATAC', 'Ten X Atac'), ('10X-CITE-feature', 'Ten X Cite Feat'), ('10X-CITE-hashing', 'Ten X Cite Hash'), ('10X-CNV', 'Ten X Cnv'), ('10X-VDJ', 'Ten X Vdj'), ('10X-VDJ-TCR', 'Ten X Vdj Tcr'), ('TSODNA', 'Tso Dna'), ('TSORNA', 'Tso Rna'), ('TsqNano', 'Tsq Nano'), ('TsqSTR', 'Tsq Str')], max_length=255)),
                ('override_cycles', models.CharField(blank=True, max_length=255, null=True)),
                ('workflow', models.CharField(choices=[('bcl', 'Bcl'), ('clinical', 'Clinical'), ('control', 'Control'), ('manual', 'Manual'), ('qc', 'Qc'), ('research', 'Research')], max_length=255)),
                ('coverage', models.CharField(blank=True, max_length=255, null=True)),
                ('truseqindex', models.CharField(blank=True, max_length=255, null=True)),
            ],
        ),
        migrations.CreateModel(
            name='Report',
            fields=[
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('subject_id', models.CharField(max_length=255)),
                ('sample_id', models.CharField(max_length=255)),
                ('library_id', models.CharField(max_length=255)),
                ('type', models.CharField(choices=[('msi', 'Msi'), ('tmb', 'Tmb'), ('tmb_trace', 'Tmb Trace'), ('fusion_caller_metrics', 'Fusion Caller Metrics'), ('failed_exon_coverage_qc', 'Failed Exon Coverage Qc'), ('sample_analysis_results', 'Sample Analysis Results'), ('target_region_coverage', 'Target Region Coverage'), ('qc_summary', 'Qc Summary'), ('multiqc', 'Multiqc'), ('report_inputs', 'Report Inputs'), ('hrd_chord', 'Hrd Chord'), ('hrd_hrdetect', 'Hrd Hrdetect'), ('purple_cnv_germ', 'Purple Cnv Germ'), ('purple_cnv_som', 'Purple Cnv Som'), ('purple_cnv_som_gene', 'Purple Cnv Som Gene'), ('sigs_dbs', 'Sigs Dbs'), ('sigs_indel', 'Sigs Indel'), ('sigs_snv_2015', 'Sigs Snv 2015'), ('sigs_snv_2020', 'Sigs Snv 2020'), ('sv_unmelted', 'Sv Unmelted'), ('sv_melted', 'Sv Melted'), ('sv_bnd_main', 'Sv Bnd Main'), ('sv_bnd_purpleinf', 'Sv Bnd Purpleinf'), ('sv_nobnd_main', 'Sv Nobnd Main'), ('sv_nobnd_other', 'Sv Nobnd Other'), ('sv_nobnd_manygenes', 'Sv Nobnd Manygenes'), ('sv_nobnd_manytranscripts', 'Sv Nobnd Manytranscripts')], max_length=255)),
                ('created_by', models.CharField(blank=True, max_length=255, null=True)),
                ('data', models.JSONField(blank=True, null=True)),
                ('s3_object_id', models.BigIntegerField(blank=True, null=True)),
                ('gds_file_id', models.BigIntegerField(blank=True, null=True)),
                ('report_uri', models.TextField(default='None')),
                ('unique_hash', data_portal.fields.HashField(base_fields=['subject_id', 'sample_id', 'library_id', 'type', 'report_uri'], default=None, null=True, unique=True)),
            ],
            options={
                'unique_together': set(),
            },
        ),
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
        migrations.CreateModel(
            name='LibraryRun',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('library_id', models.CharField(max_length=255)),
                ('instrument_run_id', models.CharField(max_length=255)),
                ('run_id', models.CharField(max_length=255)),
                ('lane', models.IntegerField()),
                ('override_cycles', models.CharField(max_length=255)),
                ('coverage_yield', models.CharField(max_length=255, null=True)),
                ('qc_pass', models.BooleanField(default=False, null=True)),
                ('qc_status', models.CharField(max_length=255, null=True)),
                ('valid_for_analysis', models.BooleanField(default=True, null=True)),
                ('workflows', models.ManyToManyField(to='data_portal.Workflow')),
            ],
            options={
                'unique_together': {('library_id', 'instrument_run_id', 'run_id', 'lane')},
            },
        ),
    ]
