# User Guide

_Required:_ You will need [curl](https://curl.se/) and [jq](https://stedolan.github.io/jq/). On macOS, you can install like so: `brew install curl jq`

## Using Portal API

### Notes

> Design Note: Portal Internal IDs are not "stable"

- By design, Portal has modelled internal ID for each record that has sync-ed or ingested into Portal database.
- This ID is also known as "Portal Internal ID".
- When context is cleared, you can use this internal ID to retrieve the said record entity. e.g. List then Get.
- However, please do note that **these internal IDs are not "stable"** nor no guarantee globally unique.
- Portal may rebuild these IDs or change its schematic nature as it sees fit and/or further expansion.

### Service Info

- [OpenAPI documentation available here](https://petstore.swagger.io/?url=https://raw.githubusercontent.com/umccr/data-portal-apis/dev/swagger/swagger.json)
- API Base URLs are as follows:
    - PROD: `https://api.portal.prod.umccr.org`
    - STG: `https://api.portal.stg.umccr.org`
    - DEV: `https://api.portal.dev.umccr.org`


## Authorization

Portal currently support 2 types of API authorization.
1. Portal Token
2. Portal IAM

### Portal Token

- Follow setting up [Portal Token](PORTAL_TOKEN.md)
- Use appropriate Portal Token depending on environment
- If you receive `Unauthorised` or similar then Portal Token has either expired or invalid token for target env
- Token valid for 24 hours (1 day)

### Portal IAM

Portal AWS IAM is for those who have access to UMCCR AWS and, need to closely knit their solution within UMCCR AWS environment. It reuses AWS SSO facility for accessing Portal APIs for conveniences.

- Follow setting up [Portal IAM](PORTAL_IAM.md)
- Can be long-live as long as AWS IAM session valid


## Endpoints

See [endpoints.md](ENDPOINTS.md)
