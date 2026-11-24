# Spec Studio relay

The Spec Studio relay is a remote MCP server that you host yourself. Claude AI (claude.ai, the Claude app, or Claude Code on another computer) uses it to list, read, make and start the specs of Spec Studio on your Mac. It also carries messages between Claude AI and the session of a spec.

The relay keeps a copy of each spec in your own Temporal namespace. So Claude AI can read the specs and leave messages while the Mac is offline. The Mac opens no port: the Spec Studio agent on the Mac connects out to the relay.

```
Claude AI ──HTTPS (OAuth)──▶ relay ◀──WebSocket── studio agent (Mac)
                               │
                 Temporal (spec mirror, inboxes, sign-ins)
                 S3 bucket (attachments)
```

## What you need

- A Temporal namespace: Temporal Cloud, or a self-hosted Temporal.
- A public HTTPS address for the relay, for example `https://relay.example.com`.
- Optional: a private S3-compatible bucket for attachments (Cloudflare R2 is the default choice).
- The owner password, as a bcrypt hash.
- The payload keys, from `relay keys`.

Run one relay process for each deploy. The relay keeps the one agent connection from the Mac in its process, so do not run two replicas of one deploy.

## Step 1: Make the secrets

Make the payload keys. The relay encrypts every Temporal payload with them. Keep a copy in a safe place: without the same keys, the relay cannot read its data. The relay refuses a payload that is not encrypted with one of its keys, so a person who can write to the namespace but has no key cannot give input to the relay's workflows.

```sh
docker run --rm ghcr.io/spec-studio-app/spec-studio-relay keys
```

Make the bcrypt hash of the owner password. You type this password on the sign-in page of the relay.

```sh
printf '%s' 'your password' | docker run --rm -i ghcr.io/spec-studio-app/spec-studio-relay password-hash
```

## Step 2: Choose the Temporal namespace

### Temporal Cloud

1. Make a namespace, and an API key for it.
2. Add the search attributes of the relay to the namespace. The relay cannot add them on Temporal Cloud. At start it prints the attributes that are missing and stops.

| Name | Type |
|---|---|
| RepoKey | Keyword |
| SpecSlug | Keyword |
| SpecTitle | Text |
| Stage | Keyword |
| Status | Keyword |
| Mode | Keyword |
| NeedsYou | Bool |
| PendingForMac | Bool |
| PendingForCoordinator | Bool |
| RelayDeployId | Keyword |

```sh
tcld namespace search-attributes add -n <namespace>.<account> --sa "RepoKey=Keyword"
# ... one line for each attribute
```

3. Set `TEMPORAL_ADDRESS` to `<namespace>.<account>.tmprl.cloud:7233` (or the regional API key endpoint), `TEMPORAL_NAMESPACE` to `<namespace>.<account>`, and `TEMPORAL_API_KEY`.

### Self-hosted Temporal

Set `TEMPORAL_ADDRESS` and `TEMPORAL_NAMESPACE`. At start, the relay registers the namespace (30 days of retention) when it does not exist, and it registers the search attributes. Use `TEMPORAL_AUTH=plain` (the default) for a frontend with no TLS, `tls` for server TLS, or `mtls` with `TEMPORAL_TLS_CERT` and `TEMPORAL_TLS_KEY`.

## Step 3: Make the bucket (optional)

Attachments go to a private bucket, never into Temporal. Without a bucket, the relay works, but messages cannot have attachments.

For Cloudflare R2:

1. Make a bucket, for example `spec-studio-relay`. Do not make it public.
2. Make an R2 API token with "Object Read & Write" on that bucket.
3. Set `BUCKET_ENDPOINT=https://<account id>.r2.cloudflarestorage.com`, `BUCKET_NAME`, `BUCKET_ACCESS_KEY` and `BUCKET_SECRET_KEY`.

Any S3-compatible service works (AWS S3, MinIO). Set `BUCKET_REGION` when the service needs a real region. The relay deletes the attachments of a spec when the spec is merged or dropped.

## Step 4: Run the relay

There are three ways. Each release publishes the image `ghcr.io/spec-studio-app/spec-studio-relay:<version>` (linux/amd64 and linux/arm64, signed with cosign) and the chart `oci://ghcr.io/spec-studio-app/charts/spec-studio-relay`.

### Docker Compose

Put the settings in a `.env` file next to `compose.yaml`. Put the password hash in single quotes, because it has `$` characters:

```sh
PUBLIC_URL=https://relay.example.com
OWNER_PASSWORD_HASH='$2b$12$...'
PAYLOAD_KEYS=data:data-20261123:...,config:config-20261123:...
BUCKET_ENDPOINT=https://<account id>.r2.cloudflarestorage.com
BUCKET_NAME=spec-studio-relay
BUCKET_ACCESS_KEY=...
BUCKET_SECRET_KEY=...
```

To try the relay with a local Temporal dev server (data in a Docker volume, Web UI on port 8233 of this machine only, because it has no sign-in):

```sh
docker compose --profile dev up
```

With your own Temporal, set `TEMPORAL_ADDRESS` (and the other Temporal settings) in `.env`, then run `docker compose up`.

Put an HTTPS proxy (Caddy, Traefik, Cloudflare Tunnel) in front of port 8760.

### Helm

```sh
helm install relay oci://ghcr.io/spec-studio-app/charts/spec-studio-relay --version <version> \
  --set publicUrl=https://relay.example.com \
  --set temporal.address=temporal-frontend.temporal.svc:7233 \
  --set temporal.namespace=studio-relay \
  --set bucket.endpoint=https://<account id>.r2.cloudflarestorage.com \
  --set bucket.name=spec-studio-relay \
  --set existingSecret=spec-studio-relay
```

The Secret `existingSecret` has the keys `OWNER_PASSWORD_HASH`, `PAYLOAD_KEYS`, and as needed `TEMPORAL_API_KEY`, `TEMPORAL_TLS_CERT`, `TEMPORAL_TLS_KEY`, `BUCKET_ACCESS_KEY`, `BUCKET_SECRET_KEY` and `AGENT_SECRET_SHA256`. Or leave `existingSecret` empty and set `secrets.*` in your values: the chart then makes the Secret. Set `ingress.enabled=true` and `ingress.host` for an Ingress. See `chart/spec-studio-relay/values.yaml` for all values.

### Docker

```sh
docker run -d --name relay -p 8760:8760 --env-file relay.env ghcr.io/spec-studio-app/spec-studio-relay:<version>
```

## Step 5: Pair the Mac

On its first start, the relay prints a pairing code in its log:

```
Pairing code: K7QM-2XRD-9VHP
It works once, for 24 hours. On the Mac, run:
  studio agent pair wss://relay.example.com/agent K7QM-2XRD-9VHP
```

Enter the relay URL and the code in Spec Studio > Settings (⌘,) > Remote Relay, or run the `studio agent pair` command. The code works once. For a new code, run `relay pair-code` where the relay runs:

```sh
docker compose exec relay relay pair-code
kubectl exec deploy/relay-spec-studio-relay -- relay pair-code
```

The relay keeps only the SHA-256 of the agent secret. Unpair in the Settings pane, or with `studio agent unpair`.

## Step 6: Add the connector in Claude AI

In Claude AI, add a custom connector with the URL `https://relay.example.com/mcp`. Sign in with the owner password. The sign-in stays after a relay restart.

The tools:

| Tool | Mac online | Mac offline |
|---|---|---|
| `list_repos` | The Mac answers. | The repos from the last connection of the Mac, with its time. |
| `list_specs` | The Mac answers. | The relay's copy of each spec, with the time of its last sync. |
| `read_spec` | The Mac answers. | The state, the turn and the last 20 events. No `spec.md` or `plan.md` text. |
| `make_spec`, `start_spec` | The Mac answers. | Fails at once. |
| `send_message` | The relay keeps the message and sends it to the Mac. | The relay keeps the message until the Mac connects. |
| `read_messages` | The messages that the sessions sent with `studio message`. | The same. |
| `upload_attachment` | Stores a file for `send_message`. | The same. |

## The Temporal Web UI

The payloads in Temporal are encrypted. The Web UI can show the payloads of the spec workflows through the codec endpoints of the relay:

1. Set `UI_ORIGIN` to the origin of the Web UI, for example `https://cloud.temporal.io` or `http://localhost:8233`.
2. Open `https://relay.example.com/codec/signin` and sign in with the owner password. The sign-in lasts 12 hours.
3. In the Web UI, set the codec endpoint to `https://relay.example.com/codec` and turn on "Include cross-origin credentials".

The codec endpoints load only the data key. The payloads of the relay-config workflow (the hashes of the sign-ins and the agent secrets) stay encrypted.

## Settings

| Setting | Required | Description |
|---|---|---|
| `PUBLIC_URL` | yes | The public origin of the relay. It is the OAuth issuer. |
| `OWNER_PASSWORD_HASH` | yes | The bcrypt hash of the owner password. `LOGIN_PASSWORD_BCRYPT` is an alias. |
| `PAYLOAD_KEYS` | yes | The codec keys, from `relay keys`. See below. |
| `TEMPORAL_ADDRESS` | yes | `host:port` of Temporal. |
| `TEMPORAL_NAMESPACE` | no | The namespace. Default `default`. |
| `TEMPORAL_AUTH` | no | `plain`, `tls`, `mtls` or `api-key`. Default: from the other Temporal settings, else `plain`. |
| `TEMPORAL_API_KEY` | no | The API key (Temporal Cloud). |
| `TEMPORAL_TLS_CERT`, `TEMPORAL_TLS_KEY` | no | The client certificate and key for mTLS: PEM text, or the path of a PEM file. |
| `DEPLOY_ID` | no | The name of this deploy. Default `default`. Two relays on one namespace need two names. |
| `TASK_QUEUE` | no | The task queue of the worker. Default `relay-<DEPLOY_ID>`. The relay-config workflow uses `<TASK_QUEUE>-config`. |
| `BUCKET_ENDPOINT`, `BUCKET_NAME`, `BUCKET_ACCESS_KEY`, `BUCKET_SECRET_KEY` | no | The attachment bucket. Set all four, or none. |
| `BUCKET_REGION` | no | The bucket region. Default `auto` (R2). |
| `ATTACHMENT_MAX_BYTES` | no | The largest attachment. Default 10485760 (10 MB). |
| `UI_ORIGIN` | no | The origin of the Temporal Web UI for the codec endpoints. |
| `AGENT_SECRET_SHA256` | no | The SHA-256 of an agent secret from before pairing. A paired secret also works. |
| `EXTRA_REDIRECT_URIS` | no | Comma-separated OAuth redirect URIs, besides the claude.ai and claude.com callbacks and loopback addresses. |
| `PORT` | no | The HTTP port. Default 8760. |
| `CALL_TIMEOUT_MS` | no | How long a tool call waits for the Mac. Default 90000. |

A setting that is missing or not valid stops the start. The message names the setting and never shows its value.

### PAYLOAD_KEYS

A comma-separated list of `<scope>:<key id>:<base64 key>`. The scope is `data` (the spec workflows) or `config` (the relay-config workflow). Each key is 32 random bytes. The first key of each scope encrypts. All keys of a scope decrypt, so you can change a key: put the new key first and keep the old key after it.

```
PAYLOAD_KEYS=data:data-2:<new>,data:data-1:<old>,config:config-1:<key>
```

## What the relay keeps in Temporal

- One workflow per spec, with the ID `spec-<repoKey>-<slug>`. `repoKey` is the first 12 hex characters of the SHA-256 of the home repo path. It holds the last state of the spec, the turn, the last 20 activity events, the messages that wait for the Mac and the messages for Claude AI. It ends when the spec is merged or dropped and no message waits. The retention period of the namespace then removes its history.
- One workflow per deploy, `relay-config-<DEPLOY_ID>`: the hashes of the agent secrets, of the pairing code and of the OAuth tokens, the OAuth clients, and the repos from the last connection of the Mac.

The relay itself keeps no state, so a restart loses no message and no sign-in.

## The relay command

```
relay [serve]     Run the relay.
relay pair-code   Make a new pairing code. The old code stops working. The relay must run.
relay keys        Print a new PAYLOAD_KEYS value.
relay password-hash
                  Read the owner password on stdin and print its bcrypt hash.
```

## Development

```sh
npm ci
npm test
```

The tests run a Temporal dev server. They use the `temporal` CLI when `TEMPORAL_CLI` names it or when it is at `/opt/homebrew/bin/temporal`. Else the Temporal SDK downloads the dev server once.

The wire contract between the relay and the Spec Studio agent (version 2) is in the Spec Studio repo, in `docs/state-format.md`.
