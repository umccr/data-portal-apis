# GDS Bundler

Create GDS folders as bundle downloader while retaining folder structure.

## Prerequisites

Generator scripts will use `ica` command. Hence, we will need valid login session context. Use either options below.

### Using ica-lazy

```
ica-add-access-token --scope read-only --project-name production
ica-context-switcher --scope read-only --project-name production
```

### Using standard

```
ica login
ica projects enter production
```

### Check token details

```
ica tokens details
```

## Export scripts

_OPTIONAL_

You may export scripts under this directory as follows. 

- Install `svn` client, if any
```
brew install svn
```

- Just export out `gds_bundle` scripts (e.g. for staging a bundle for some data sharing task)
```
cd ~/tmp
svn export https://github.com/umccr/data-portal-apis.git/trunk/docs/examples/gds_bundle tothill_cup_tso_bundle_20230311
cd tothill_cup_tso_bundle_20230311
tree -a
.
├── .gitignore
├── README.md
├── gds_folders.txt
├── gen_bundle.sh
└── gen_downloader.sh

1 directory, 5 files
```

This is optional. You may wish to directly download the scripts and prepare the data staging whichever way you prefer.

Note that [.gitignore](.gitignore) exclusion. If those exclusion is not applicable to your case, make adjustment accordingly or simply remove it. 

## Generate Bundle

### Prepare GDS folder list

- Create text file with GDS folder list
```
gds_folders.txt
```

### Generate Bundle

GDS folders bundle will be represented as [ndjson](http://ndjson.org/)

```
bash gen_bundle.sh <my_bundle_prefix>
```

e.g.

```
bash gen_bundle.sh tothill_cup
```

This should produce `tothill_cup_bundle.ndjson`

### Bundle Format

Each line of `tothill_cup_bundle.ndjson` represents some files of a GDS folder.

```
[{"path":"/analysis_data/SBJ03116/tso_ctdna_tumor_only/202303046e1d1813/L2300278/Results/MetricsOutput.tsv","presignedUrl":"https://..."}, ...]
[{"path":"/analysis_data/SBJ03117/tso_ctdna_tumor_only/202303046af1e936/L2300280/Results/MetricsOutput.tsv","presignedUrl":"https://..."}, ...]
...
```

Each line is a JSON array (a folder) of some JSON Objects (files) as in shape as follows.

```
Array[]:
    Object{}:
        path: string
        presignedUrl: string
```

We could distribute this ndjson bundle to end user; assuming the user know how to fiddle with the bundle file for maximum flexibility or however they'd like. Otherwise, a downloader script generator is provided as an example; next section.

## Generate Downloader

Knowing _Bundle Format_; the generator script will prepare downloader script; to use with `curl` for PreSigned URLs. Files will be downloaded as in the path structure.

Pass-in the bundle ndjson from previous section as first argument to the generator script.

```
bash gen_downloader.sh <my_bundle.ndjson>
```

e.g.

```
bash gen_downloader.sh tothill_cup_bundle.ndjson
```

This should create `tothill_cup_bundle.downloader`. You may QA/verify the generated downloader script, if any. Adjust the generator script as required; rinse & spin.

### Run Downloader Script

This `tothill_cup_bundle.downloader` contains just shell commands in the script. So, we can simply execute as follows.

```
sh tothill_cup_bundle.downloader
```

It will start `curl` download of files. For most part, it is recommended to run the downloader script in `screen` or `tmux` session; or some form of OS background task; if applicable.

For simpler; we could rather distribute this `tothill_cup_bundle.downloader` script to end user; who would just simply run it.


## Parallel

There are potentially slice the data partition by lines in either `tothill_cup_bundle.ndjson` step or, even `tothill_cup_bundle.downloader` step for maximum data transfer concurrency.

You may normally do this in some (Gadi/Spartan) HPC interactive session. If you would like to utilise multiple
nodes for the task, you can further split the ndjson or downloader file by lines as follows:

e.g. 8 folders per task
```
split -l 8 tothill_cup_bundle.ndjson
```

e.g. 200 files per task
```
split -l 200 tothill_cup_bundle.downloader
```

Though, it is recommended to split the bundle ndjson for better cohesion by folder structure. Splitting downloader script would create scattered files around different download processes.

Then you can request nodes to download them in parallel. 

Observe split files like so:

```
less xaa
less xab
```

Or, consider using [GNU Parallel](https://www.gnu.org/software/parallel/) to wrap around as see fit.
