# Portal Release

Portal maintains 3 environments and data stages (pipelines & cloud objects that are tracked): development, staging, production.


### Steps

- Create "Release Sprint" in Trello
- Create "Release Milestone" in GitHub repos: backend, frontend, infrastructure
- Create "Release Directory" and follow ValidationSOP outline at https://drive.google.com/drive/u/1/folders/1ozmDlGvJFC5vkN7HsIXNp9sYDJHZmvdN
- Link relevant Trello cards, GitHub PRs & issues in sync
- Create 2 chore cards in Trello: Backend Chores ([example 1](https://trello.com/c/DuPmROYN/1527-portal-release-ocicat-backend-chores)), Frontend Chores ([example 2](https://trello.com/c/iwV5EFfp/1528-portal-release-ocicat-frontend-chores)) to capture routine maintenance updates
- Be proactive and communicate with relevant stakeholders; consolidate deliverable feature tickets (major, minor)
- Portal practise [GitOps](../README.md); GitHub Pull Request review approval ([example](https://github.com/umccr/data-portal-apis/pull/647)) to `main` branch is the final technical tick for deployment to production 
  - i.e. Automated CI/CD process follows thereafter and, no human-manual handling involve with deployment 
- Hence, make sure ValidationSOP are carried out before merging the final release PR to repo `main` branch
- After release has deployed, 
  - follow up with documentation update
  - tag repos
  - create GitHub releases from tag


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

### Portal Development

- Portal development environment is temporary or ephemeral
- Application is built from repo `dev` branch latest HEAD commit; or any commit points as see fit
- We often recycle and restore from snapshot of development database (hence, ephemeral storage)
- We also delete Portal Pipeline Automation driven data from Cloud buckets/volumes without any further advanced notice ([example](https://umccr.slack.com/archives/CCC5J2NM6/p1699332559638679))

### Portal Staging

- Portal Staging maintain UAT controlled environment 
- We often mount/copy the data from production on case by case basis 
- Portal Staging retains the data stages (Workflow runs, Cloud data objects) that is tracked

### Portal Production

- Portal Production is controlled and highly regulated environment
- There are enforced SOP in order to operate in the Portal Production by trained person

### Operational Tasks

- Handling Portal DLQ
- Handling Portal Database
- Handling Portal Release Validation Runs
- Handling Portal Automated Workflow Runs (reprocessing requests; including automated Cohort data processing, rerun historical samples, etc.)
- Handling Portal Controlled Data Deletion

Often, these operational tasks are communicated and ChatOps through Slack channel `#data-portal` and/or Trello service request cards.
