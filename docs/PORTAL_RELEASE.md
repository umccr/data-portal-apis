# Portal Release

Portal maintains 3 environments and data stages (pipelines & cloud objects that are tracked): development, staging, production.


### Steps

- Create "Release Sprint" in Trello
- Create "Release Milestone" in GitHub repos: backend, frontend, infrastructure
- Create "Release Directory" and follow ValidationSOP outline at https://drive.google.com/drive/u/1/folders/1ozmDlGvJFC5vkN7HsIXNp9sYDJHZmvdN
- Link relevant Trello cards, GitHub PRs & issues in sync
- Create 2 chore cards in Trello: Backend Chores ([example 1](https://trello.com/c/DuPmROYN/1527-portal-release-ocicat-backend-chores)), Frontend Chores ([example 2](https://trello.com/c/iwV5EFfp/1528-portal-release-ocicat-frontend-chores)) to capture routine maintenance updates
- Be proactive and communicate with relevant stakeholders; consolidate deliverable feature tickets (major, minor)
- Portal practise [GitOps](../README.md); GitHub Pull Request approval to `main` branch ([example](https://github.com/umccr/data-portal-apis/pull/647)) is the final technical tick for deployment to production 
  - i.e. Automated CI/CD process follows thereafter and, no human-manual handling involve with deployment 
- Hence, make sure ValidationSOP and change approvals are carried out before merging the final release PR to repo `main` branch
  - perform "Release Validation Protocol - Template"; seek team members helps and distribute tasks, if any
  - perform validation runs in Portal Staging for UAT purpose
- After release has deployed,
  - tag repos
  - create GitHub releases from tag
  - follow up with documentation update
- After major release have done,
  - give 1+ month "code freeze" time window 
  - allow only documentation and/or bug fixes merges to `dev` branch
  - block off all major feature branches merging into `dev` branch; keep them in their own feature branches 
    - _Remember: you can deploy from any commit point in Portal Development environment_ (see below)
  - priority during code freeze period are any work that deems bug-fix, hot-fix, documentation, minor feature fast track 
  - perform "Release Backporting", if any (see below)
  - observe release stability:
    - 1 or 2 months
    - 2 to 4 sequencing runs have completed
    - communication with stakeholders & reporting manager 
  - open next sprint iteration once the release become stable; rinse & spin


#### Milestones
- https://github.com/umccr/data-portal-apis/milestones?state=closed
- https://github.com/umccr/data-portal-client/milestones?state=closed
- https://github.com/umccr/infrastructure/milestones?state=closed

#### Tags
- https://github.com/umccr/data-portal-apis/tags
- https://github.com/umccr/data-portal-client/tags

#### GitHub Releases
- https://github.com/umccr/data-portal-apis/releases
- https://github.com/umccr/data-portal-client/releases

#### Release Backporting

Unless advised otherwise, perform "Release Backporting Protocol" as outline in ValidationSOP document.

As follows:

- Go to https://drive.google.com/drive/u/1/folders/1ozmDlGvJFC5vkN7HsIXNp9sYDJHZmvdN
- "Release Backporting Protocol - Template" - the backporting template that you can copy / base off from
- "Release Backporting Protocol - Example" - with example contents for reference and expectation

The process and reporting is slightly differ from the major "Release Validation Protocol - Template"; such that it tailors toward tracking "non-impactful" changes, security fixes, hot-fixes, some minor features to fast track.

The "backporting" is one of software engineering practice. Recommended to have a bit of [background reading](https://www.google.com/search?q=backporting). So that we understood the concept better and, we would not exploit the flexibility the process that offer. We still have do best practise with care; not to overdo with changes during "backporting" time window. Only security issues related are always prioritised and backported throughout major release product life cycle. Features are not.

It is recommended that, after a major release has been made into production, make some minor updates within 1 month time window. Our goal is "**to have a robust / well tested system operation in production**". Work with stakeholder on "change requests" into classifying major or minor priority; then decide whether to backport or, next iteration as per your expertise and work item estimation.

Unless security concern; after 1 month or so window period over, you should defer any change requests to next iteration. However, keep this option open and negotiable. It depends on business value and work item priority. Practise your soft skill here.

After next iteration has commenced and some progress has made in `dev` branch; if you must merge commits to `main` due to security or hotfix, you can use git cherrypick or, work on hotfix branch and merge back (PR) to all `dev`, `stg`, `main` branches. Hence, backported.

Give some time and practise with a toy git repo, if this git flow is new for you.


### Portal Development

- Portal development environment is temporary or ephemeral.
- Application is built from repo `dev` branch latest HEAD commit; or any commit points as see fit.
- We often recycle and restore from snapshot of development database (hence, ephemeral storage).
- We also delete Portal Pipeline Automation driven data from Cloud buckets/volumes without any further advanced notice ([example](https://umccr.slack.com/archives/CCC5J2NM6/p1699332559638679)).
- If there is need, we tear down the whole Portal stack in dev; and re-deploy.
- Perform all experimentation, debugging, rapid testing and verification runs shall be done in dev.
- Once you can conclude sudden level of stability, promote changes to Staging.
  - Before promoting Release Candidate (RC) changes to Staging, work with team member whether to harmonise the RC releases.

### Portal Staging

- Portal Staging maintain UAT (User Acceptance Test) controlled environment.
- We often mount/copy the data from production on case by case basis.
- At this point, we handle all CAB requests loop:
  - Create RC PR ([example](https://github.com/umccr/data-portal-apis/pulls?q=is%3Apr+is%3Aclosed)) to Staging.
  - Establish communication with User on feature & bug-fix UAT.
  - Go back to dev > prepare another update to code > promote another RC (rinse & spin) until UAT pass.
  - Handle all stakeholder change management process.
  - Carry out ValidationSOP mention above and, all stakeholders should have been signed off on changes.
- Portal Staging retains the data stages (Workflow runs, Cloud data objects) that is tracked. 
- Portal Staging maintains data consistency as in production fashion. However, this is not as strictly enforced as in production.

### Portal Production

- Portal Production is controlled and highly regulated environment.
- There are enforced SOP in order to operate in the Portal Production by trained person.

### Operational Tasks

- Handling Portal DLQ
- Handling Portal Database
- Handling Portal Release Validation Runs
- Handling Portal Automated Workflow Runs (reprocessing requests; including automated Cohort data processing, rerun historical samples, etc.)
- Handling Portal Controlled Data Deletion

Often, these operational tasks are communicated and ChatOps through Slack channel `#data-portal` and/or Trello service request cards.

### Deletion Request

Unless advised otherwise, as follows.

To delete data that is tracked and produced in Portal Staging and Production:

- Start a new post in Slack channel `#bioinfo` for data deletion request 
- State your reasons and any justification (e.g. incorrect input, duplicate run, etc.)
- Communicate with stakeholders of concern on data that is requested for deletion, if any
- Wait for cooling-off period retention (depends on circumstances - sensitivity, reasoning, complexity, etc.)
