# Bugler authentication

## Create the first admin key

```bash
bun run bugler init --db bugler.db
```

The command prints one `bgl_adm_` token once. Store it in a secret manager. Bugler stores only its SHA-256 hash.

```bash
export BUGLER_ADMIN_TOKEN='bgl_adm_...'
bun run bugler serve --db bugler.db
```

Use the admin token as an HTTP bearer credential:

```bash
curl http://127.0.0.1:8787/sources \
  -H "Authorization: Bearer $BUGLER_ADMIN_TOKEN"
```

Admin lifecycle endpoints are `POST /admin-keys`, `GET /admin-keys`, `PATCH /admin-keys/{id}`, and `DELETE /admin-keys/{id}`. Rotation supports an overlap window. Tokens never belong in query strings.

## Authenticate a subscriber

`POST /subscriptions` returns one `bgl_sub_` token once. Each subscription is one logical consumer and one cursor.

```bash
export BUGLER_TOKEN='bgl_sub_...'
bun run bugler listen --relay http://127.0.0.1:8787 --sub pr-42-agent -- your-handler
```

For direct HTTP calls, send the same token as `Authorization: Bearer $BUGLER_TOKEN`. For MCP, run:

```bash
bun run bugler mcp --relay http://127.0.0.1:8787 --sub pr-42-agent
```

Subscriber rotation is `POST /subscriptions/{id}/token/rotate`; revocation is `DELETE /subscriptions/{id}/token`. Agents acknowledge with the `cursor_epoch` returned by delivery.

## Environment conventions

- `BUGLER_ADMIN_TOKEN`: operator-side admin token used by curl or tooling.
- `BUGLER_TOKEN`: subscriber token used by `listen` and `mcp`.
- `BUGLER_BOOTSTRAP_ADMIN_TOKEN`: cloud bootstrap token, supplied through the platform secret store.
- `env:NAME` and `file:PATH`: stored secret references for sources and push destinations.

For one compatibility release, `REVEILLE_*` inputs are deprecated aliases and checksummed `rvl_` tokens still verify. Bugler prefers `BUGLER_*` and warns once when it uses a legacy variable.

## Hosted (early access)

The hosted control plane is coming. Intended flow:

1. Sign in with Clerk at the hosted Bugler control plane.
2. Create or select a relay.
3. Create, rotate, revoke, and label API keys through self-service.
4. Copy each show-once key into the human's secret manager or the agent runtime's environment.
5. Use the same bearer-token and cursor-epoch contracts as self-hosted Bugler.

Hosted access does not change the relay's payload trust model. Events remain untrusted data.
