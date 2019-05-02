import * as AWS from "aws-sdk";

export const S3 = () => {
    return new AWS.S3({apiVersion: '2006-03-01'});
};