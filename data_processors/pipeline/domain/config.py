# -*- coding: utf-8 -*-
"""module for pipeline constants

Let's be Pythonic 💪 let's not mutate CAPITAL_VARIABLE elsewhere!
Consider Enum, if there's a need for (name, value) and better protected tuple pair.
Or consider Helper class-ing where composite builder is needed.
"""

ICA_WORKFLOW_PREFIX = "/iap/workflow"

SQS_NOTIFICATION_QUEUE_ARN = "/data_portal/backend/sqs_notification_queue_arn"

SQS_TN_QUEUE_ARN = "/data_portal/backend/sqs_tumor_normal_queue_arn"
SQS_DRAGEN_WGS_QC_QUEUE_ARN = "/data_portal/backend/sqs_dragen_wgs_qc_queue_arn"
SQS_DRAGEN_TSO_CTDNA_QUEUE_ARN = "/data_portal/backend/sqs_dragen_tso_ctdna_queue_arn"
SQS_UMCCRISE_QUEUE_ARN = "/data_portal/backend/sqs_umccrise_queue_arn"
SQS_DRAGEN_WTS_QUEUE_ARN = "/data_portal/backend/sqs_dragen_wts_queue_arn"
SQS_RNASUM_QUEUE_ARN = "/data_portal/backend/sqs_rnasum_queue_arn"
SQS_SOMALIER_EXTRACT_QUEUE_ARN = "/data_portal/backend/sqs_somalier_extract_queue_arn"
SQS_STAR_ALIGNMENT_QUEUE_ARN = "/data_portal/backend/sqs_star_alignment_queue_arn"

# SSM parameter names for external submission lambdas
STAR_ALIGNMENT_LAMBDA_ARN = "/nextflow_stack/star-align-nf/submission_lambda_arn"
