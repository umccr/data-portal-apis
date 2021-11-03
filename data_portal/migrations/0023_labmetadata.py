# Generated by Django 3.1.8 on 2021-04-28 01:52

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_portal', '0022_merge_20210411_0614'),
    ]

    operations = [
        migrations.CreateModel(
            name='LabMetadata',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
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
    ]