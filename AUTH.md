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

`POST /subscriptions` is create-only and returns one `bgl_sub_` token once. Reusing an existing subscription id returns 409 without changing its token, cursor, epoch, filter, action, transport, or principal. Each subscription is one logical consumer and one cursor.

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

Source mapping has an intentionally mixed schema: `mapping.source` is a required string, while `type`, `subject`, `dedupekey`, and `time` are mapping objects using exactly one of `static`, `header`, `json_pointer`, or `template`. The generic non-preset example in README is the canonical copyable shape.

For one compatibility release, `REVEILLE_*` inputs are deprecated aliases and checksummed `rvl_` tokens still verify. Bugler prefers `BUGLER_*` and warns once when it uses a legacy variable.

## Hosted (early access)

The hosted control plane is available to early-access accounts at `https://app.bugler.dev`. Flow:

1. Sign in with Clerk at the hosted Bugler control plane.
2. The first authenticated request provisions an isolated relay automatically.
3. Mint a show-once platform API key through `POST /api/platform-keys` for agent automation.
4. Copy each show-once key into the human's secret manager or the agent runtime's environment.
5. Use the same bearer-token and cursor-epoch contracts as self-hosted Bugler.

Hosted access does not change the relay's payload trust model. Events remain untrusted data.

### Browser sign-in and automation keys

Humans should sign in at `https://app.bugler.dev/`, then use **Mint platform key** in the dashboard. The admin token created with the first tenant and every newly minted platform key are each shown once with a copy button. Store them before leaving the page.

An agent operating an already signed-in browser can obtain the short-lived Clerk session token with:

```js
await window.Clerk.session.getToken()
```

Use that value as `Authorization: Bearer <token>` only long enough to mint a platform key. Everything after platform-key minting can use ordinary `curl` without browser state.

### Local daemon sessions

Create a mint-only credential with a Clerk session or platform key. The token is shown once:

```bash
export BUGLER_PLATFORM_TOKEN='bgl_plt_replace_me'
curl -fsS https://app.bugler.dev/api/daemon-credentials \
  -H "Authorization: Bearer $BUGLER_PLATFORM_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"subscription_ids":["github-starter"]}'
```

Import its returned `bgl_dmn_` token by environment-variable name:

```bash
export BUGLER_DAEMON_TOKEN='bgl_dmn_replace_me'
bugler daemon init --account work --origin https://relay.bugler.dev \
  --tenant tenant_123 --principal user_123 --credential-env BUGLER_DAEMON_TOKEN
```

The daemon stores it in a mode-0600 account file and scrubs `BUGLER_DAEMON_TOKEN`. It mints short-lived `bgl_dvc_` transport credentials with `POST` at `/t/{tenant}/daemon/sessions`. Neither token is accepted by event, history, cursor, source, billing, or management routes. Rotate with `POST /api/daemon-credentials/{id}/rotate`; revoke with `DELETE /api/daemon-credentials/{id}`.

For a plain OSS relay, omit `--credential-env`. Store one subscriber token per mirrored subscription without putting it in argv:

```bash
export BUGLER_SUB_TOKEN='bgl_sub_replace_me'
bugler local mirror add --account work --sub agent-events \
  --token-env BUGLER_SUB_TOKEN
```

The same `--token-env` input is required for hosted mirrors. The daemon uses these scoped subscriber-token files only when its preferred hosted WebSocket falls back to per-subscription long-poll. Each file is private to the account configuration directory and mode 0600. A missing token is surfaced as `fallback_unavailable` by `bugler daemon status --json`.

The local daemon mints a show-once `bgl_lmg_` management capability during init and a show-once `bgl_lcon_` consume capability per local consumer. Management capabilities configure mirrors, consumers, actions, recovery, and local tokens. Consume capabilities bind one consumer and may read or acknowledge only that local stream. Use `BUGLER_LOCAL_TOKEN` for `bugler local events` and `bugler mcp --account`.

### Organizations, roles, and enterprise SSO

Select the active Clerk organization before using the hosted API. Each organization maps to one tenant. Bugler synchronizes Clerk organization membership webhooks and recognizes `owner`, `admin`, and `member`. Owners and admins manage sources, grants, service principals, and billing. Members create subscriptions and see only upstream-resolved or explicitly granted events.

Enterprise SAML or OIDC is configured on the Clerk organization connection. Keep issuer metadata, client credentials, and connection identifiers in Clerk and Worker secrets. Bugler does not hardcode identity-provider configuration.

### Add GitHub with one click

Signing in through Clerk with GitHub automatically records the Clerk-verified GitHub user id and username with provenance `clerk-social`. This pre-link is identity metadata only. It contains no GitHub token and grants no event or repository entitlement. Email and Google sign-ins do not create a GitHub pre-link.

In the dashboard, select **Monitor repos with the GitHub App**, choose an organization and repositories on GitHub, then approve the App. GitHub returns to a confirmation page whose script submits the authorization to `POST /api/github/install/complete`. Loading or prefetching the callback page performs no state change. On first setup, completion links the installing member, creates the tenant's GitHub App source and monitored-repository set, creates a `github-starter` pull subscription for pull request, review, and check events, and reveals its subscriber token once with a copy button. A later install completion updates the caller's GitHub identity and monitored set but returns the existing starter without its token. It does not rotate the token, reset the cursor or epoch, or reassign the subscription principal. Use the explicit subscriber token rotation endpoint when rotation is intended. No webhook URL or secret is copied.

Hosted `POST /api/subscriptions` is also create-only. Caller-chosen ids remain supported, but an existing id returns 409 and `github-starter` is reserved for the GitHub App install flow. No create path can rotate a token or change cursor state. Use `POST /subscriptions/{id}/token/rotate` only when rotation is intended.

An agent can start the same browser handoff with a member-scoped Clerk session or platform API key:

```bash
GITHUB_INSTALL=$(curl -sS -X POST https://app.bugler.dev/api/github/install/start \
  -H "Authorization: Bearer $BUGLER_PLATFORM_KEY")
printf '%s\n' "$GITHUB_INSTALL" | jq -r '.installation_url'
```

Open the returned URL and complete GitHub installation and authorization. Agents handling the GitHub redirect themselves may send its `code`, `installation_id`, and `state` as JSON directly to `POST /api/github/install/complete`. The one-time state expires after 10 minutes. Completion leases it briefly, releases it after a failed dependency call so the identical request can be retried, and consumes it only after all setup commits. A replay after success returns 401. Bugler validates that the authorized GitHub user can access the returned installation before linking it to the tenant. Bugler stores opaque token references in the identity row, keeps user and refresh tokens in tenant-local secret storage, and rotates expiring user tokens through GitHub OAuth. GitHub repository visibility is refreshed every 15 minutes and invalidated immediately by installation, repository, member, and membership webhooks.

When the authorized GitHub user id matches the Clerk social pre-link, Bugler merges them and preserves provenance `clerk-social`. If the ids differ, Bugler retains the social pre-link and marks the explicitly authorized account as active for entitlements. Inspect the safe identity summary with `GET /api/identity-links/github`. Use **Re-link GitHub identity** or `POST /api/identity-links/github/start` to change or refresh the authorized account.

Signing in with GitHub does not grant repo access. The GitHub App user-to-server token created by install or explicit authorization remains mandatory for entitlement resolution.

### Add Stripe, Sentry, GitLab, or CircleCI

Choose the provider under **Add another source**. Enter its webhook secret or token once. Bugler returns one `ingest_url`. Paste that URL into the provider's webhook URL field. Stripe uses Developers > Webhooks, Sentry uses Developer Settings > Internal Integrations > Webhooks, and GitLab and CircleCI use the project's Webhooks settings. No relay-side mapping code is needed for these presets.

### Grants and service principals

An owner or admin can grant one source, one source plus subject prefix, or all sources:

```bash
curl -sS -X POST https://app.bugler.dev/api/grants \
  -H "Authorization: Bearer $BUGLER_PLATFORM_KEY" \
  -H "Content-Type: application/json" \
  --data '{"principal_kind":"member","principal_id":"user_123","source":"stripe/acme","subject_prefix":"team/payments/"}'

SERVICE=$(curl -sS -X POST https://app.bugler.dev/api/service-principals \
  -H "Authorization: Bearer $BUGLER_PLATFORM_KEY" \
  -H "Content-Type: application/json" \
  --data '{"name":"payments-reconciler"}')
export BUGLER_SERVICE_KEY=$(printf '%s' "$SERVICE" | jq -r '.token')
export BUGLER_SERVICE_ID=$(printf '%s' "$SERVICE" | jq -r '.service_principal.id')
```

Members and service principals default to no event visibility. Owner and admin access is tenant-wide. Subscriber tokens and platform API keys retain their hosted principal, so delivery, SSE, history, MCP polling, and push composition apply the same entitlement intersection.

The exact hosted curl sequence is served as machine-readable Markdown at `https://app.bugler.dev/auth.md` and `https://bugler.dev/auth.md`.
