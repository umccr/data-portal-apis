import * as AWS from "aws-sdk";
import {sleep} from "./utils";

export const call = (action, params) => {
    const athena = new AWS.Athena({apiVersion: '2017-05-18'});

    return athena[action](params).promise();
};
export const getDataForQuery = async (queryString, resultSetParser = null) => {
    const params = {
        QueryString: queryString,
        ResultConfiguration: {
            EncryptionConfiguration: {
                EncryptionOption: "SSE_S3"
            },
            OutputLocation: "s3://umccr-athena-query-results-dev/"
        },
    };

    const startQueryExecutionResult = await call("startQueryExecution", params);
    const queryExecutionId = startQueryExecutionResult.QueryExecutionId;

    await waitForQueryToComplete(queryExecutionId);

    const queryResults = await call("getQueryResults", {QueryExecutionId: queryExecutionId});

    if (resultSetParser) {
        return resultSetParser(queryResults.ResultSet.Rows);
    }

    return queryResults.ResultSet.Rows;
};

const waitForQueryToComplete = async queryExecutionId => {
    let isQueryStillRunning = true;

    while (isQueryStillRunning) {
        const getQueryExecutionResult = await call("getQueryExecution",{QueryExecutionId: queryExecutionId});
        const queryState = getQueryExecutionResult.QueryExecution.Status.State;
        const stateChangeReason = getQueryExecutionResult.QueryExecution.Status.StateChangeReason;

        if (queryState === "SUCCEEDED") {
            isQueryStillRunning = false;
        } else if (queryState === "FAILED") {
            throw "Query was cancelled: " + stateChangeReason
        } else if ( queryState === "CANCELLED") {
            throw "Query was cancelled: " + stateChangeReason
        } else {
            await sleep(1000);
        }
    }
};

export const DATA_TABLE_NAME = 'dafu.data';
export const LIMS_TABLE_NAME = 'google_lims.umccr_data_google_lims_dev';