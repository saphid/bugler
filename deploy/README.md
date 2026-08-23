# Deployment routing guard

The public OSS Worker is a self-hosted relay and must never own a `bugler.dev` route. Its `wrangler.toml` enables only `workers_dev`.

Hosted releases must deploy the closed platform Worker from `platform/wrangler.toml`. That configuration is the only owner of `app.bugler.dev`, `relay.bugler.dev`, and `api.bugler.dev`, plus the exact `bugler.dev/auth.md` route. Before deployment, verify Wrangler's route plan and confirm that no other Worker claims those hostnames. The platform Worker also checks `BUGLER_EXPECTED_HOSTS` and returns 421 for a mismatched host.
