# Domain Event Schema

Portal domain event JSON schemas

You can use these JSON schema to programmatically generate event payload construct. Some examples as follows.

## Python

- https://docs.pydantic.dev/latest/integrations/datamodel_code_generator/

_(from project root)_

```bash
datamodel-codegen --input docs/schemas/WorkflowRunStateChange.schema.json --input-file-type jsonschema --output data_processors/pipeline/domain/event/wrsc.py
```

- See model serde unit test case for using generated Model

```bash
python manage.py test data_processors.pipeline.domain.event.tests.test_wrsc.WRSCUnitTests.test_model_serde
```

## Others

JSON Schema to X

- https://transform.tools/json-schema-to-openapi-schema
- https://transform.tools/json-schema-to-typescript
