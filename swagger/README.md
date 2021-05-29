# OpenAPI

This directory contains OpenAPI/Swagger JSON files. They are generated/obtained as follows.

## Data Portal

OpenAPI schema is auto-generated at:
- http://localhost:8000/swagger.json
- http://localhost:8000/swagger.yaml

This is then downloaded into `swagger/swagger.json`
```
curl -s http://localhost:8000/swagger.json | jq > swagger/swagger.json
```

Or, make shortcut:
```
make openapi
```

## ICA

Sync of https://github.com/umccr-illumina/libica/tree/dev/swagger

```
wget https://raw.githubusercontent.com/umccr-illumina/libica/dev/swagger/console.json -O console.json
wget https://raw.githubusercontent.com/umccr-illumina/libica/dev/swagger/ens.json -O ens.json
wget https://raw.githubusercontent.com/umccr-illumina/libica/dev/swagger/gds.json -O gds.json
wget https://raw.githubusercontent.com/umccr-illumina/libica/dev/swagger/tes.json -O tes.json
wget https://raw.githubusercontent.com/umccr-illumina/libica/dev/swagger/wes.json -O wes.json
```
