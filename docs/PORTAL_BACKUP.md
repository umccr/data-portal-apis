# Portal Backup

## Database

> How is the Portal backend database backed up?

Portal database has 2 tiers backup setup.

### Tier 1
- Short-term; fine-grain database backup using [RDS automated backup](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_ManagingAutomatedBackups.html) service.
- 7 days point-in-time (PIT) retention.

### Tier 2
- Longer-term; RDS database instance backup using [AWS Backup](https://docs.aws.amazon.com/prescriptive-guidance/latest/backup-recovery/aws-backup.html) service.
- Backup weekly snapshot and keeping it for 6 weeks retention.

### Recovery Point Objective

By a combination of 2 tier backup setup, we can effectively restore database to any _point-in-time_ within past 7 days. After day 7, we can restore the database to its weekly state; upto past 6 weeks. (See [RPO](https://www.google.com/search?q=recovery+point+objective)). By leveraging multiple backup services, it systematically increases backup redundancy.

### Encryption

- RDS automated backups are encrypted and, database instance is deletion protected.
- AWS Backup Vault is encrypted with KMS.

### Operation

- Database maintenance and restore operations (DBA tasks) are carried out by trained person. (See [PORTAL_RELEASE.md](PORTAL_RELEASE.md))
