# easyAPI

A TypeScript library for quickly building JSONB-first CRUD APIs on PostgreSQL with Express.

It provides:
- a single API endpoint (`POST /api/clientAPI`)
- model-based permissions
- JSON Schema validation
- pre/post operation listeners
- built-in login/logout with session tokens
- automatic table/index setup from JSON config

## Repository layout

- `postgres/easyAPI/easyAPI.ts` — main API runtime (`EasyAPI`, `createEasyAPI`)
- `postgres/easyAPI/jsonSchemaToPostgres.ts` — JSON config → PostgreSQL table/index migration
- `postgres/easyAPI/types.ts` — shared types and enums

## Install

```bash
npm install express pg jsonschema uuid
npm install -D typescript @types/express @types/pg @types/node
```

## Quick start

```ts
import express from 'express';
import { Pool } from 'pg';
import { createEasyAPI } from './postgres/easyAPI/easyAPI';
import { APIOperation, AuthState, EasyAPIConfig } from './postgres/easyAPI/types';

const app = express();
app.use(express.json());

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

const config: EasyAPIConfig = {
  userModel: 'users',
  apiModels: [
    {
      modelName: 'users',
      primaryKey: 'userID',
      permissions: {
        read: AuthState.AUTHENTICATED,
        write: AuthState.OPEN,
        update: AuthState.AUTHENTICATED,
        delete: AuthState.AUTHENTICATED
      },
      dataOperations: [
        APIOperation.READ,
        APIOperation.WRITE,
        APIOperation.UPDATE,
        APIOperation.DELETE
      ],
      nonReadableFields: ['password', 'salt'],
      jsonSchema: {
        type: 'object',
        required: ['email', 'password'],
        properties: {
          userID: { type: 'string', readOnly: true },
          email: { type: 'string' },
          password: { type: 'string' },
          salt: { type: 'string', readOnly: true },
          created: { type: 'string', readOnly: true },
          updated: { type: 'string', readOnly: true }
        }
      },
      tableConfig: {
        tableName: 'users',
        indexes: [
          { type: 'btree', fields: ['email'], unique: true },
          { type: 'btree', fields: ['userID'], unique: true }
        ]
      }
    }
  ]
};

async function start() {
  const easyAPI = await createEasyAPI(pool, config);
  app.use(easyAPI.getRouter());
  app.listen(3000, () => console.log('Server running on :3000'));
}

start().catch(console.error);
```

## API endpoint

All client operations use one endpoint:

```http
POST /api/clientAPI
Content-Type: application/json
```

Request shape:

```json
{
  "operation": "read | write | update | delete | login | logout",
  "model": "modelName",
  "data": {},
  "query": {},
  "token": "optional-session-token",
  "meta": {
    "pagination": {
      "limit": 50,
      "offset": 0,
      "orderBy": "created",
      "orderDirection": "DESC"
    }
  }
}
```

Response shape:

```json
{
  "success": true,
  "message": "optional",
  "data": {},
  "isAuthorized": true,
  "meta": {
    "pagination": {
      "total": 120,
      "limit": 50,
      "offset": 0,
      "hasMore": true
    }
  }
}
```

## Supported operations

- `read`
- `write`
- `update`
- `delete`
- `login`
- `logout`

### Notes

- `update` requires a `query` that includes the model primary key.
- `delete` requires a non-empty `query`.
- `write` auto-generates:
  - primary key (UUIDv7)
  - `created` timestamp
  - `updated` timestamp
- `update` auto-updates `updated` timestamp.
- For the configured `userModel`, passwords are hashed with PBKDF2 + salt.

## Query language

Basic equality:

```json
{ "query": { "clientID": "abc123" } }
```

Nested fields:

```json
{ "query": { "address.city": "NYC" } }
```

Operators:

- `$gt`, `$gte`, `$lt`, `$lte`
- `$ne`
- `$in`
- `$contains` (JSONB containment)
- `$or`

Example:

```json
{
  "query": {
    "$or": [
      { "status": "active" },
      { "score": { "$gte": 80 } }
    ]
  }
}
```

## Permissions and auth

Each model defines permissions per operation:

- `AuthState.OPEN`
- `AuthState.AUTHENTICATED`

`login` creates a session token in the internal `sessions` table. Use that token in subsequent requests.

## Listeners

Per-model hooks are available:

- pre/post read
- pre/post write
- pre/post update
- pre/post delete

A listener returns:

```ts
{ success: boolean; message?: string }
```

If a pre-listener returns `success: false`, the operation is blocked.

## Server-side helper methods

`EasyAPI` also provides direct helpers (without HTTP):

- `writeData(modelName, data, user?)`
- `readData(modelName, query?, user?)`
- `readDataPaginated(modelName, query?, pagination?, user?)`
- `updateData(modelName, query, data, user?)`
- `deleteData(modelName, query, user?)`

## Table/index migration utility

`JsonSchemaToPostgres` can create/update table structures using `TableConfig`, including:

- JSONB `data` column
- generated columns from JSON paths
- index generation (`btree`, `gin`, `hash`, expression)
- optional full-text search vector/index

## Current limitations / assumptions

- Validation happens in app layer (`jsonschema`), not DB constraints.
- The main API route is fixed at `/api/clientAPI`.
- SQL identifiers come from config; keep model and field config trusted.

## License

See [LICENSE](LICENSE).
