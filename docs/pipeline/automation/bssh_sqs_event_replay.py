import ast
import json
from datetime import datetime, timezone
from urllib.parse import urlparse, ParseResult

import requests
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth

# Usage:
#   export AWS_PROFILE=prod
#   python data/make_event.py

runs = [
    ["230602_A00130_0258_BH55TMDSX7", "r.EgZe1CMokEKBAOInwt7WvQ"],
    ["230608_A01052_0153_AH53JWDSX7", "r.ZbhluFYOykKAkMl6lztRwg"],
    ["230629_A01052_0154_BH7WF5DSX7", "r.BDX5uxzx7EGVAVNiUHhcaw"],
    ["230707_A00130_0264_BH7WJTDSX7", "r.Doo1NoNLkUedSP9Pud3ukQ"],
    ["230714_A01052_0158_AH7WGHDSX7", "r.4zqT3NKkxUulz6mMCP3RkA"]
]


# ---


def get_event_tpl():
    event_tpl = {
        "Records": [
            {
                "messageId": "b2b8b423-3dd8-4aea-bbf2-f13108eaeb20",
                "receiptHandle": "AQEB+PZKPwzpzgx7sc...LSVcWz4nlHxsj0X",  # pragma: allowlist secret
                "body": None,
                "attributes": {
                    "ApproximateReceiveCount": "2",
                    "SentTimestamp": "1637706092924",
                    "SenderId": "AROARFCPI2IGX3HSTQ5:5cb0370d-7d39-4bb1-8108-ba27121643c4",  # pragma: allowlist secret
                    "ApproximateFirstReceiveTimestamp": "1637706092925"
                },
                "messageAttributes": {
                    "subscription-urn": {
                        "stringValue": "urn:ilmn:igp:us-east-1:YX.NQ:subscription:sub.1025",  # pragma: allowlist secret
                        "stringListValues": [],
                        "binaryListValues": [],
                        "dataType": "String"
                    },
                    "contentversion": {
                        "stringValue": "v1",
                        "stringListValues": [],
                        "binaryListValues": [],
                        "dataType": "String"
                    },
                    "action": {
                        "stringValue": "statuschanged",
                        "stringListValues": [],
                        "binaryListValues": [],
                        "dataType": "String"
                    },
                    "actiondate": {
                        "stringValue": None,
                        "stringListValues": [],
                        "binaryListValues": [],
                        "dataType": "String"
                    },
                    "type": {
                        "stringValue": "bssh.runs",
                        "stringListValues": [],
                        "binaryListValues": [],
                        "dataType": "String"
                    },
                    "producedby": {
                        "stringValue": "BaseSpaceSequenceHub",
                        "stringListValues": [],
                        "binaryListValues": [],
                        "dataType": "String"
                    },
                    "contenttype": {
                        "stringValue": "application/json",
                        "stringListValues": [],
                        "binaryListValues": [],
                        "dataType": "String"
                    }
                },
                "md5OfMessageAttributes": "18e31cf8c7e82c7901a7d29b0bd589d",  # pragma: allowlist secret
                "md5OfBody": "fc38bcadba65d115d4ed663219b9e0c",  # pragma: allowlist secret
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:ap-southeast-2:472057503814:data-portal-prod-iap-ens-event-queue",
                "awsRegion": "ap-southeast-2"
            }
        ]
    }

    return event_tpl


def get_bssh_event(instrument_run_id_, run_id_, status="PendingAnalysis"):
    obj: ParseResult = urlparse("https://api.data.prod.umccr.org/iam/sequencerun")

    auth = BotoAWSRequestsAuth(
        aws_host=obj.hostname,
        aws_region="ap-southeast-2",
        aws_service="execute-api"
    )

    resp: requests.Response = requests.get(
        url=obj.geturl(),
        auth=auth,
        params={
            'instrument_run_id': instrument_run_id_,
            'run_id': run_id_,
            'status': status,
        }
    )

    return resp.json()


if __name__ == '__main__':

    for run in runs:
        instrument_run_id = run[0]
        run_id = run[1]
        event = get_bssh_event(instrument_run_id_=instrument_run_id, run_id_=run_id)

        results = event['results']
        assert len(results) > 0
        # print(results[0])

        bssh_event = results[0]
        # print(bssh_event)

        date_modified = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        body = {
            "gdsFolderPath": bssh_event['gds_folder_path'],
            "gdsVolumeName": bssh_event['gds_volume_name'],
            "reagentBarcode": bssh_event['reagent_barcode'],
            "v1pre3Id": str(bssh_event['v1pre3_id']),
            "dateModified": date_modified,
            "acl": ast.literal_eval(bssh_event['acl']),
            "flowcellBarcode": bssh_event['flowcell_barcode'],
            "sampleSheetName": bssh_event['sample_sheet_name'],
            "apiUrl": bssh_event['api_url'],
            "name": bssh_event['name'],
            "id": bssh_event['run_id'],
            "instrumentRunId": bssh_event['instrument_run_id'],
            "status": "PendingAnalysis"
        }

        sqs_event = get_event_tpl()
        sqs_event['Records'][0]['body'] = json.dumps(body)
        sqs_event['Records'][0]['messageAttributes']['actiondate']['stringValue'] = date_modified

        with open(f"{instrument_run_id}/bssh_event.json", "w") as out:
            json.dump(sqs_event, out)
