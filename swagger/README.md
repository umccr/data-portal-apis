# OpenAPI

This directory contains OpenAPI/Swagger JSON files. They are generated/obtained as follows.

We typically generate/sync after any major release as part of post-release documentation follow up.

## Portal

After `make start`, the Portal OpenAPI schema is generated at:
- http://localhost:8000/swagger.json
- http://localhost:8000/swagger.yaml

This is then download into `swagger/swagger.json`
```
curl -s http://localhost:8000/swagger.json | jq > swagger/swagger.json
```

Or, shortcut:
```
make start
make openapi
```

## ICA

Sync of https://github.com/umccr-illumina/libica/tree/dev/swagger

```
cd swagger/
wget https://raw.githubusercontent.com/umccr-illumina/libica/dev/swagger/console.json -O console.json
wget https://raw.githubusercontent.com/umccr-illumina/libica/dev/swagger/ens.json -O ens.json
wget https://raw.githubusercontent.com/umccr-illumina/libica/dev/swagger/gds.json -O gds.json
wget https://raw.githubusercontent.com/umccr-illumina/libica/dev/swagger/tes.json -O tes.json
wget https://raw.githubusercontent.com/umccr-illumina/libica/dev/swagger/wes.json -O wes.json
wget https://raw.githubusercontent.com/umccr-illumina/libica/main/swagger/openapi_public.yaml -O openapi_public.yaml
```
