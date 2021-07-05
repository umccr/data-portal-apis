# -*- coding: utf-8 -*-
"""module for pipeline constants

Let's be Pythonic 💪 let's not mutate CAPITAL_VARIABLE elsewhere!
Consider Enum, if there's a need for (name, value) and better protected tuple pair.
Or consider Helper class-ing where composite builder is needed.
"""

ICA_GDS_FASTQ_VOL = "/iap/gds/fastq_vol"
ICA_WORKFLOW_PREFIX = "/iap/workflow"

SQS_TN_QUEUE_ARN = "/data_portal/backend/sqs_tumor_normal_queue_arn"
SQS_GERMLINE_QUEUE_ARN = "/data_portal/backend/sqs_germline_queue_arn"
SQS_NOTIFICATION_QUEUE_ARN = "/data_portal/backend/sqs_notification_queue_arn"
