// -*- coding: utf-8 -*-
/**
 Usage:
   yarn add aws4-axios axios aws-sdk
   export AWS_PROFILE=prodops
   node portal_api_sig4.js
 **/
import axios from 'axios';
import AWS from 'aws-sdk';
import {aws4Interceptor} from 'aws4-axios';

// https://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/setting-credentials-node.html
const credentials = new AWS.SharedIniFileCredentials();

const interceptor = aws4Interceptor(
  {
    region: 'ap-southeast-2',
    service: 'execute-api',
  },
  credentials
);

axios.interceptors.request.use(interceptor);

axios.get('https://api.data.prod.umccr.org/iam/lims').then((res) => {
  console.log(res.data)
});
