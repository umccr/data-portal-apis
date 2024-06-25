# Portal LibraryID Suffix Treatment

At Portal-driven Bioinformatics workflow automation, our current convention is; 

- We strip all Lab annotated suffixes `_topup` and `_rerun` at orchestration input preparation step from the LibraryID.
- We only use _original_ LibraryID for all downstream "Secondary Analysis" workflows.
- Lab annotated suffixes on LibraryID are terminated at "Primary Analysis" stage i.e. 
  - only upto BCL Conversion and, 
  - FASTQ file naming and,
  - when populating Portal database `FastqListRow` table.

## FastqListRow Table

Portal `FastqListRow` table systematically maintains this mapping as last known point before starting downstream informatics analysis. This table keeps track of _suffix_ vs _original_ LibraryID by each sequencing runs. As follows.

Observe:

```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/fastq?rglb=L2100191" | jq
```

```
select * from "data_portal_fastqlistrow" where "rglb" = 'L2100191';
```

It resolves:

> rglb = always stripped/minted to original library id
> e.g. `L2100191`

> rgid = index1.index2.lane.instrumentRunId.sampleId_libaryId_AsInLabSampleSheet
> e.g. `AXXXXXXT.TYYYYYYG.1.450325_A01052_0038_AHYGWMDSXY.MDX410045_L2100191_rerun`

```
{
  "rgid": "AXXXXXXT.TYYYYYYG.1.450325_A01052_0038_AHYGWMDSXY.MDX410045_L2100191_rerun",
  "rgsm": "MDX410045",
  "rglb": "L2100191",
  "lane": 1,
  "read_1": "gds://.../450325_A01052_0038_AHYGWMDSXY/<portal_run_id>/.../MDX410045_L2100191_rerun_S1_L002_R1_001.fastq.gz",
  "read_2": "gds://.../450325_A01052_0038_AHYGWMDSXY/<portal_run_id>/.../MDX410045_L2100191_rerun_S1_L002_R2_001.fastq.gz"
}
```

## By Design Effects

### Merge Effect

> Merge Effect 1:
> 
> If LibraryID comes later as multiple topup(s) at different sequencing point-in-time, query by `rglb` gives "merging effect" of all these topup sequencing run of the same _root_ LibraryID.

```
L2100191         \
L2100191_topup2   \
...                -->  L2100191
L2100191_topup3   /
L2100191_topup4  /
```

> Merge Effect 2:
> 
> If LibraryID split into multiple lanes, query by `rglb` gives "merging effect" of all these sequencing run FASTQs by the same _root_ LibraryID.

```
L2100241 Lane 1  \
L2100241 Lane 2   \
...                -->  L2100241
L2100241 Lane 3   /
L2100241 Lane 4  /
```

A combination of both merge effects 1 and 2 is possible as a total product.

Observe API (WGS Library):
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/fastq?rglb=L2100241" | jq
```

Observe UI (WGS Library):
- https://portal.umccr.org/subjects/SBJ00723/launch-pad/wgs-tn

Observe API (WTS Library):
```
curl -s -H "Authorization: Bearer $PORTAL_TOKEN" "https://api.portal.prod.umccr.org/fastq?rglb=L2400882" | jq
```

### Override Effect

> Override Effect:
> 
> If LibraryID comes later as intention to override previous sequenced LibraryID, query by `rglb` gives "override effect". 

```
L2100191_rerun  -- override -->  L2100191
L2100191_rerun2 -- override -->  L2100191
```

---

See also:
- https://github.com/umccr/data-portal-apis/issues/678
- https://github.com/umccr/biodaily/pull/69
- https://umccr.slack.com/archives/C06KER1H5D0/p1717381146413379
