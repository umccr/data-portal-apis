// -*- coding: utf-8 -*-
/**
 Usage:
 yarn add aws4-axios axios @aws-sdk/credential-providers
 export AWS_PROFILE=prod
 node portal_api_sig4.js
 **/
import axios from 'axios';
import {fromIni} from '@aws-sdk/credential-providers';
import {aws4Interceptor} from 'aws4-axios';

// https://docs.aws.amazon.com/sdk-for-javascript/v3/developer-guide/setting-credentials-node.html
const credentialsProvider = fromIni();
// console.log(await credentialsProvider())

const interceptor = aws4Interceptor({
    options: {
        region: 'ap-southeast-2',
        service: 'execute-api',
    },
    credentials: await credentialsProvider(),
});

axios.interceptors.request.use(interceptor);

axios.get('https://api.portal.prod.umccr.org/iam/lims').then((res) => {
    console.log(res.data)
});
