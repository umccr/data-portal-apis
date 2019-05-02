import {S3} from "./libs/s3-lib";
import {badRequest, failure, success} from "./libs/response-lib";

export const main = async (event, context) => {
    const queryStringParams = event.queryStringParameters;
    const bucket = queryStringParams.bucket;
    const key = queryStringParams.key;

    if (bucket === null || key === null) {
        return badRequest({errors: 'missing required parameters'});
    }

    const s3 = S3();

    try {
        const url = s3.getSignedUrl('getObject', {
            Bucket: bucket,
            Key: key,
        });

        return success(url);
    } catch (e) {
        return failure({errors: e});
    }
};