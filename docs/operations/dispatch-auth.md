# Dispatch auth header guidance

## Jobs API vs session API

Dispatch uses two different auth surfaces:

- Jobs API endpoints use `X-API-Key`
  - examples: `/v1/jobs`, `/v1/jobs/{id}`, `/v1/jobs/{id}/logs`, `/v1/queue`
- Session/user/project endpoints use `Authorization: Bearer <access_token>`
  - examples: `/v1/login`, `/v1/projects`, `/v1/users`, `/v1/projects/{project_id}/api-keys`

## Operator hints

Use these rules when debugging auth:

- If you are calling jobs endpoints, use `X-API-Key: <api_key>`
- If you are calling login/project/session endpoints, use `Authorization: Bearer <access_token>`

For additive compatibility, jobs endpoints also accept:

- `Authorization: Bearer <api_key>`
- `Authorization: ApiKey <api_key>`

That compatibility path exists to reduce operator confusion, but the canonical jobs header remains `X-API-Key`.

## CLI helper

To print the same guidance from the CLI:

- `dispatch doctor --auth-hints`
- `dispatch server doctor --auth-hints`
