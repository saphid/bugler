#!/usr/bin/env bash
set -euo pipefail

RESOURCE_GROUP=''
IMAGE=''
MODE=''
LOCATION='eastus'
APP_NAME='bugler'
KEY_VAULT_OVERRIDE=''
DRY_RUN=0
YES=0

usage() {
  printf 'Usage: %s --resource-group NAME --image IMAGE --mode scale-to-zero|always-on [--location REGION] [--app-name NAME] [--key-vault NAME] [--yes] [--dry-run]\n' "$0"
}

lowercase() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]'
}

while (($# > 0)); do
  case "$1" in
    --resource-group) RESOURCE_GROUP=${2:?--resource-group requires a value}; shift ;;
    --image) IMAGE=${2:?--image requires a value}; shift ;;
    --mode) MODE=${2:?--mode requires a value}; shift ;;
    --location) LOCATION=${2:?--location requires a value}; shift ;;
    --app-name) APP_NAME=${2:?--app-name requires a value}; shift ;;
    --key-vault) KEY_VAULT_OVERRIDE=${2:?--key-vault requires a value}; shift ;;
    --yes) YES=1 ;;
    --dry-run) DRY_RUN=1 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'Unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

if [[ -z "$RESOURCE_GROUP" || -z "$IMAGE" || ("$MODE" != 'scale-to-zero' && "$MODE" != 'always-on') ]]; then
  usage >&2
  exit 2
fi

if [[ "$MODE" == 'scale-to-zero' ]]; then
  MIN_REPLICAS=0
  COST_NOTE="min replicas 0 targets the monthly free grants, but cold starts can miss GitHub's 10 second deadline"
else
  MIN_REPLICAS=1
  COST_NOTE="min replicas 1 is the reliable profile at about \$3 to \$5 per month when mostly idle"
fi

COSMOS_ACCOUNT="${APP_NAME//_/}-cosmos"
COSMOS_DATABASE='bugler'
COSMOS_CONTAINER='relay'
ENVIRONMENT="${APP_NAME//_/}-env"
KEY_VAULT=${KEY_VAULT_OVERRIDE:-"${APP_NAME//_/}-kv"}
if [[ ! "$KEY_VAULT" =~ ^[a-zA-Z][a-zA-Z0-9-]{1,22}[a-zA-Z0-9]$ ]]; then
  printf '%s\n' '--key-vault must be 3 to 24 letters, numbers, or hyphens, starting with a letter and ending with a letter or number' >&2
  exit 2
fi

if ((DRY_RUN)); then
  printf '%s\n' \
    'DRY RUN: az account show' \
    "DRY RUN: create or reuse resource group $RESOURCE_GROUP in $LOCATION" \
    "DRY RUN: create or reuse Cosmos free-tier account $COSMOS_ACCOUNT with shared 1000 RU/s database and /partitionKey container" \
    "DRY RUN: create or reuse Container Apps environment $ENVIRONMENT" \
    "DRY RUN: check global Key Vault name availability for $KEY_VAULT" \
    "DRY RUN: check live and soft-deleted Key Vault name $KEY_VAULT before mutation" \
    'DRY RUN: recover only a matching soft-deleted vault; never purge a protected vault' \
    "DRY RUN: create or update Container App $APP_NAME from $IMAGE with $COST_NOTE" \
    'DRY RUN: enable the app system identity and grant Key Vault secret-read access' \
    'DRY RUN: stream the Cosmos key and prompted bootstrap admin token into Key Vault without argv or persistent files' \
    'DRY RUN: az containerapp secret set using a Key Vault reference, never a raw secret' \
    'DRY RUN: update BUGLER_STORAGE=cosmos and secretref environment values' \
    "Cost mode: $COST_NOTE"
  exit 0
fi

if ((YES == 0)); then
  printf '%s\n' '--yes is required because this command changes Azure resources' >&2
  exit 2
fi

command -v az >/dev/null 2>&1 || { printf '%s\n' 'Azure CLI is required' >&2; exit 1; }
az account show >/dev/null

az group create --name "$RESOURCE_GROUP" --location "$LOCATION" >/dev/null
if ! az cosmosdb show --resource-group "$RESOURCE_GROUP" --name "$COSMOS_ACCOUNT" >/dev/null 2>&1; then
  az cosmosdb create --resource-group "$RESOURCE_GROUP" --name "$COSMOS_ACCOUNT" \
    --locations regionName="$LOCATION" failoverPriority=0 isZoneRedundant=False \
    --enable-free-tier true --default-consistency-level Session >/dev/null
fi
if ! az cosmosdb sql database show --resource-group "$RESOURCE_GROUP" --account-name "$COSMOS_ACCOUNT" --name "$COSMOS_DATABASE" >/dev/null 2>&1; then
  az cosmosdb sql database create --resource-group "$RESOURCE_GROUP" --account-name "$COSMOS_ACCOUNT" \
    --name "$COSMOS_DATABASE" --throughput 1000 >/dev/null
fi
if ! az cosmosdb sql container show --resource-group "$RESOURCE_GROUP" --account-name "$COSMOS_ACCOUNT" \
  --database-name "$COSMOS_DATABASE" --name "$COSMOS_CONTAINER" >/dev/null 2>&1; then
  az cosmosdb sql container create --resource-group "$RESOURCE_GROUP" --account-name "$COSMOS_ACCOUNT" \
    --database-name "$COSMOS_DATABASE" --name "$COSMOS_CONTAINER" --partition-key-path /partitionKey >/dev/null
fi

if ! az containerapp env show --resource-group "$RESOURCE_GROUP" --name "$ENVIRONMENT" >/dev/null 2>&1; then
  az containerapp env create --resource-group "$RESOURCE_GROUP" --name "$ENVIRONMENT" --location "$LOCATION" >/dev/null
fi
LIVE_VAULT_GROUP=$(az keyvault show --name "$KEY_VAULT" --query resourceGroup --output tsv 2>/dev/null || true)
VAULT_NAME_AVAILABLE=$(az keyvault check-name --name "$KEY_VAULT" --query nameAvailable --output tsv)
LIVE_VAULT_GROUP_LOWER=$(lowercase "$LIVE_VAULT_GROUP")
RESOURCE_GROUP_LOWER=$(lowercase "$RESOURCE_GROUP")
if [[ -n "$LIVE_VAULT_GROUP" && "$LIVE_VAULT_GROUP_LOWER" != "$RESOURCE_GROUP_LOWER" ]]; then
  printf 'Key Vault name %s is already live in resource group %s. Re-run with --key-vault and a globally unique name.\n' "$KEY_VAULT" "$LIVE_VAULT_GROUP" >&2
  exit 1
fi
if [[ -z "$LIVE_VAULT_GROUP" ]]; then
  DELETED_VAULT_ID=$(az keyvault list-deleted --query "[?name=='$KEY_VAULT'] | [0].properties.vaultId" --output tsv)
  if [[ -n "$DELETED_VAULT_ID" ]]; then
    DELETED_VAULT_GROUP=$(printf '%s' "$DELETED_VAULT_ID" | cut -d/ -f5)
    DELETED_VAULT_GROUP_LOWER=$(lowercase "$DELETED_VAULT_GROUP")
    if [[ "$DELETED_VAULT_GROUP_LOWER" != "$RESOURCE_GROUP_LOWER" ]]; then
      printf 'Key Vault name %s is soft-deleted from resource group %s and may be purge-protected. It will not be purged. Re-run with --key-vault and another globally unique name.\n' "$KEY_VAULT" "$DELETED_VAULT_GROUP" >&2
      exit 1
    fi
    az keyvault recover --name "$KEY_VAULT" >/dev/null
  else
    VAULT_NAME_AVAILABLE_LOWER=$(lowercase "$VAULT_NAME_AVAILABLE")
    if [[ "$VAULT_NAME_AVAILABLE_LOWER" != 'true' ]]; then
      printf 'Key Vault name %s is unavailable outside this subscription. Re-run with --key-vault and another globally unique name.\n' "$KEY_VAULT" >&2
      exit 1
    fi
    az keyvault create --resource-group "$RESOURCE_GROUP" --name "$KEY_VAULT" --location "$LOCATION" --enable-rbac-authorization false >/dev/null
  fi
fi

if ! az containerapp show --resource-group "$RESOURCE_GROUP" --name "$APP_NAME" >/dev/null 2>&1; then
  az containerapp create --resource-group "$RESOURCE_GROUP" --name "$APP_NAME" --environment "$ENVIRONMENT" \
    --image "$IMAGE" --ingress external --target-port 8787 --system-assigned \
    --cpu 0.25 --memory 0.5Gi --min-replicas "$MIN_REPLICAS" --max-replicas 1 \
    --env-vars BUGLER_STORAGE=cosmos >/dev/null
else
  az containerapp update --resource-group "$RESOURCE_GROUP" --name "$APP_NAME" --image "$IMAGE" \
    --cpu 0.25 --memory 0.5Gi --min-replicas "$MIN_REPLICAS" --max-replicas 1 >/dev/null
fi

az containerapp identity assign --resource-group "$RESOURCE_GROUP" --name "$APP_NAME" --system-assigned >/dev/null
PRINCIPAL_ID=$(az containerapp identity show --resource-group "$RESOURCE_GROUP" --name "$APP_NAME" --query principalId --output tsv)
az keyvault set-policy --name "$KEY_VAULT" --object-id "$PRINCIPAL_ID" --secret-permissions get >/dev/null

az cosmosdb keys list --resource-group "$RESOURCE_GROUP" --name "$COSMOS_ACCOUNT" --type keys \
  --query primaryMasterKey --output tsv | tr -d '\r\n' | \
  az keyvault secret set --vault-name "$KEY_VAULT" --name bugler-cosmos-key --file /dev/stdin --encoding utf-8 >/dev/null

if ! az keyvault secret show --vault-name "$KEY_VAULT" --name bugler-bootstrap-admin-token >/dev/null 2>&1; then
  printf '%s' 'Enter a valid bgl_adm_ bootstrap token. Input is hidden: ' >&2
  IFS= read -r -s BOOTSTRAP_TOKEN
  printf '\n' >&2
  if [[ ! "$BOOTSTRAP_TOKEN" =~ ^bgl_adm_ ]]; then
    printf '%s\n' 'Bootstrap token must start with bgl_adm_' >&2
    exit 1
  fi
  printf '%s' "$BOOTSTRAP_TOKEN" | az keyvault secret set --vault-name "$KEY_VAULT" \
    --name bugler-bootstrap-admin-token --file /dev/stdin --encoding utf-8 >/dev/null
  unset BOOTSTRAP_TOKEN
fi

COSMOS_SECRET_URI=$(az keyvault secret show --vault-name "$KEY_VAULT" --name bugler-cosmos-key --query id --output tsv)
BOOTSTRAP_SECRET_URI=$(az keyvault secret show --vault-name "$KEY_VAULT" --name bugler-bootstrap-admin-token --query id --output tsv)
az containerapp secret set --resource-group "$RESOURCE_GROUP" --name "$APP_NAME" --secrets \
  "cosmos-key=keyvaultref:$COSMOS_SECRET_URI,identityref:system" \
  "bootstrap-admin=keyvaultref:$BOOTSTRAP_SECRET_URI,identityref:system" >/dev/null

COSMOS_ENDPOINT=$(az cosmosdb show --resource-group "$RESOURCE_GROUP" --name "$COSMOS_ACCOUNT" --query documentEndpoint --output tsv)
az containerapp update --resource-group "$RESOURCE_GROUP" --name "$APP_NAME" --set-env-vars \
  BUGLER_STORAGE=cosmos \
  "BUGLER_COSMOS_ENDPOINT=$COSMOS_ENDPOINT" \
  BUGLER_COSMOS_KEY=secretref:cosmos-key \
  "BUGLER_COSMOS_DATABASE=$COSMOS_DATABASE" \
  "BUGLER_COSMOS_CONTAINER=$COSMOS_CONTAINER" \
  BUGLER_BOOTSTRAP_ADMIN_TOKEN=secretref:bootstrap-admin >/dev/null

FQDN=$(az containerapp show --resource-group "$RESOURCE_GROUP" --name "$APP_NAME" --query properties.configuration.ingress.fqdn --output tsv)
printf 'Bugler URL: https://%s\n' "$FQDN"
printf 'Cost mode: %s\n' "$COST_NOTE"
printf '%s\n' 'Container Apps ingress caps requests at 240 seconds. Keep relay waits below that boundary.'
