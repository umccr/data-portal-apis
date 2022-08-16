// see "Use JSON Expression" https://illumina.gitbook.io/ica-v1/events/e-jsonexpressions

// Write JSON expression and output for ready to use in subscription payload. Required Node.js.
// Usage:
//      node expr.js

let gds_dev_expr =
    {
        "or": [
            {"equal": [{"path": "$.volumeName"}, "umccr-primary-data-dev"]},
            {"equal": [{"path": "$.volumeName"}, "umccr-run-data-dev"]},
            {"equal": [{"path": "$.volumeName"}, "umccr-fastq-data-dev"]},
            {
                "and": [
                    {"equal": [{"path": "$.volumeName"}, "development"]},
                    {
                        "or": [
                            {"startsWith":[{"path":"$.path"},"/analysis_data/"]},
                            {"startsWith":[{"path":"$.path"},"/primary_data/"]}
                        ]
                    }
                ]
            }
        ]
    };

console.log(JSON.stringify(JSON.stringify(gds_dev_expr)));

let gds_prod_expr =
    {
        "or": [
            {"equal": [{"path": "$.volumeName"}, "umccr-primary-data-prod"]},
            {"equal": [{"path": "$.volumeName"}, "umccr-run-data-prod"]},
            {"equal": [{"path": "$.volumeName"}, "umccr-fastq-data-prod"]},
            {
                "and": [
                    {"equal": [{"path": "$.volumeName"}, "production"]},
                    {
                        "or": [
                            {"startsWith":[{"path":"$.path"},"/analysis_data/"]},
                            {"startsWith":[{"path":"$.path"},"/primary_data/"]}
                        ]
                    }
                ]
            }
        ]
    };

console.log(JSON.stringify(JSON.stringify(gds_prod_expr)));

let wes_run_name_expr = {"startsWith":[{"path":"$.WorkflowRun.Name"},"umccr__automated"]};

console.log(JSON.stringify(JSON.stringify(wes_run_name_expr)));
