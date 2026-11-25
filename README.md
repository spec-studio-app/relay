# Spec Studio relay

The Spec Studio relay is a remote MCP server that you host yourself. Claude AI (claude.ai, the Claude app, or Claude Code on another computer) uses it to list, read, make and start the specs of Spec Studio on your Mac. It also carries messages between Claude AI and the session of a spec.

New to the relay? The [relay guide](https://spec-studio-app.github.io/site/guides/relay/) takes you from no relay to Claude AI reading a spec, and [Relay backends](https://spec-studio-app.github.io/site/guides/backends/) explains each store, bucket and hosting choice. This README is the full reference.

The relay keeps a copy of each spec, so Claude AI can read the specs and leave messages while the Mac is offline. It keeps the copy in one of two stores:

- **The JSON store** (the default): JSON files on a volume. The relay is one container and a volume, with no other service.
- **The Temporal store**: your own Temporal namespace (Temporal Cloud or self-hosted). Use it when you already run Temporal.

The Mac opens no port: the Spec Studio agent on the Mac connects out to the relay.

```
Claude AI ──HTTPS (OAuth)──▶ relay ◀──WebSocket── studio agent (Mac)
                               │
                 JSON files on a volume, or Temporal (spec mirror, inboxes, sign-ins)
                 S3 bucket (attachments, optional)
```

## What you need

- A public HTTPS address for the relay, for example `https://relay.example.com`.
- The owner password, as a bcrypt hash.
- For the JSON store: a persistent volume for the relay's data folder.
- For the Temporal store: a Temporal namespace, and the payload keys from `relay keys`.
- Optional: a private S3-compatible bucket for attachments (Cloudflare R2 is the default choice).

Run one relay process for each deploy. The relay keeps the one agent connection from the Mac in its process, so do not run two replicas of one deploy. With the JSON store, the state is in the files of that one process too.

## Step 1: Make the password hash

Make the bcrypt hash of the owner password. You type this password on the sign-in page of the relay.

```sh
printf '%s' 'your password' | docker run --rm -i ghcr.io/spec-studio-app/relay password-hash
```

## Step 2: Choose the store

`RELAY_STORE` selects the store: `json` or `temporal`. When it is not set, the relay uses the Temporal store if `TEMPORAL_ADDRESS` is set, else the JSON store. At start, the relay logs the store and the reason (`store_selected`).

### The JSON store (the default)

The relay keeps its state in `RELAY_DATA_DIR` (default `/data`). Mount a persistent volume there: without a volume, the state is lost with the container. The image runs as uid 1000, and the folder must be writable by that user. When it is not, the relay stops at start with a message that names `RELAY_DATA_DIR`.

There is nothing more to set up.

### The Temporal store

Set `TEMPORAL_ADDRESS` (or `RELAY_STORE=temporal` with `TEMPORAL_ADDRESS`), `PAYLOAD_KEYS`, and the other Temporal settings below.

Make the payload keys. The relay encrypts every Temporal payload with them. Keep a copy in a safe place: without the same keys, the relay cannot read its data. The relay refuses a payload that is not encrypted with one of its keys. Each update input also names its workflow and the time it was made, and the workflows refuse a copy for another workflow or one older than 5 minutes. So a person who can write to the namespace but has no key cannot make new input for the relay's workflows; they can only send a copy of a recent update again to the same workflow.

```sh
docker run --rm ghcr.io/spec-studio-app/relay keys
```

#### Temporal Cloud

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

3. Set `TEMPORAL_ADDRESS` to `<namespace>.<account>.tmprl.cloud:7233` (or the regional API key endpoint), `TEMPORAL_NAMESPACE` to `<namespace>.<account>`, and `TEMPORAL_API_KEY`. For mTLS instead of an API key, set `TEMPORAL_TLS_CERT` and `TEMPORAL_TLS_KEY`. The relay knows Temporal Cloud from the API key or from a `*.tmprl.cloud` address. On Temporal Cloud it never registers a namespace or search attributes: it prints each missing attribute with its type and stops.

#### Self-hosted Temporal

Set `TEMPORAL_ADDRESS` and `TEMPORAL_NAMESPACE`. At start, the relay registers the namespace (30 days of retention) when it does not exist, and it registers the search attributes. Use `TEMPORAL_AUTH=plain` (the default) for a frontend with no TLS, `tls` for server TLS, or `mtls` with `TEMPORAL_TLS_CERT` and `TEMPORAL_TLS_KEY`.

### Switch between stores

The relay does not move data from one store to the other. A relay on a new store starts empty:

- The Mac fills the new store with a short first sync of each spec (its state, its last 20 activity lines and its messages for Claude AI) when it has no sync cursors. So on the Mac, run `studio agent unpair --force` (it removes the pairing and the cursors on this Mac only), then pair again (Step 5).
- The messages that wait for the Mac in the old store are lost, and so are the messages for Claude AI that it did not read. So switch when no message waits: the Mac is online and took every message, and Claude AI read its messages.
- The pairing and the sign-ins are in the store too. After the switch, sign in to the connector in Claude AI again.

An upgrade from relay 0.3 keeps Temporal: the old settings set `TEMPORAL_ADDRESS`, so the relay uses the Temporal store. With the Compose `dev` profile of 0.3, add `TEMPORAL_ADDRESS=temporal:7233` to `.env`: the 0.4 `compose.yaml` no longer sets it.

## Step 3: Make the bucket (optional)

Attachments go to a private bucket, never into the store. Without a bucket, the relay works, but messages cannot have attachments.

For Cloudflare R2:

1. Make a bucket, for example `spec-studio-relay`. Do not make it public.
2. Make an R2 API token with "Object Read & Write" on that bucket.
3. Set `BUCKET_ENDPOINT=https://<account id>.r2.cloudflarestorage.com`, `BUCKET_NAME`, `BUCKET_ACCESS_KEY` and `BUCKET_SECRET_KEY`.

Any S3-compatible service works (AWS S3, MinIO). Set `BUCKET_REGION` when the service needs a real region. The relay deletes the attachments of a spec when the spec is merged or dropped.

## Step 4: Run the relay

There are three ways. Each release publishes the image `ghcr.io/spec-studio-app/relay:<version>` (linux/amd64 and linux/arm64, signed with cosign) and the chart `oci://ghcr.io/spec-studio-app/charts/relay`.

### Docker Compose

Put the settings in a `.env` file next to `compose.yaml`. Put the password hash in single quotes, because it has `$` characters:

```sh
PUBLIC_URL=https://relay.example.com
OWNER_PASSWORD_HASH='$2b$12$...'
BUCKET_ENDPOINT=https://<account id>.r2.cloudflarestorage.com
BUCKET_NAME=spec-studio-relay
BUCKET_ACCESS_KEY=...
BUCKET_SECRET_KEY=...
```

Then run:

```sh
docker compose up
```

The relay uses the JSON store, with its data in the Docker volume `relay-data`.

With your own Temporal, also set `TEMPORAL_ADDRESS`, `PAYLOAD_KEYS` and the other Temporal settings in `.env`. To try the Temporal store with a local Temporal dev server (data in a Docker volume, Web UI on port 8233 of this machine only, because it has no sign-in), set `TEMPORAL_ADDRESS=temporal:7233` and `PAYLOAD_KEYS` in `.env`, then run:

```sh
docker compose --profile dev up
```

Put an HTTPS proxy (Caddy, Traefik, Cloudflare Tunnel) in front of port 8760.

### Helm

With the JSON store (the default), the chart makes a PersistentVolumeClaim (`persistence.size`, default 1Gi) and keeps it when the release is removed:

```sh
helm install relay oci://ghcr.io/spec-studio-app/charts/relay --version <version> \
  --set publicUrl=https://relay.example.com \
  --set bucket.endpoint=https://<account id>.r2.cloudflarestorage.com \
  --set bucket.name=spec-studio-relay \
  --set existingSecret=relay-secrets
```

Set `persistence.existingClaim` to use a claim that you made, and `persistence.storageClass` for a storage class. The chart runs one replica with the `Recreate` strategy, so an old pod and a new pod never write the files at the same time.

With Temporal:

```sh
helm install relay oci://ghcr.io/spec-studio-app/charts/relay --version <version> \
  --set publicUrl=https://relay.example.com \
  --set store=temporal \
  --set temporal.address=temporal-frontend.temporal.svc:7233 \
  --set temporal.namespace=studio-relay \
  --set existingSecret=relay-secrets
```

The Secret `existingSecret` has the key `OWNER_PASSWORD_HASH`, and as needed `PAYLOAD_KEYS` (Temporal), `TEMPORAL_API_KEY`, `TEMPORAL_TLS_CERT`, `TEMPORAL_TLS_KEY`, `BUCKET_ACCESS_KEY` and `BUCKET_SECRET_KEY`. Or leave `existingSecret` empty and set `secrets.*` in your values: the chart then makes the Secret. Set `ingress.enabled=true` and `ingress.host` for an Ingress. See `chart/relay/values.yaml` for all values. When `store` is empty, the chart uses the relay's rule: `temporal` when `temporal.address` is set, else `json`.

### Docker

```sh
docker run -d --name relay -p 8760:8760 -v relay-data:/data --env-file relay.env ghcr.io/spec-studio-app/relay:<version>
```

## Step 5: Pair the Mac

On its first start, the relay prints a pairing code in its log:

```
Pairing code: K7QM-2XRD-9VHP
It works once, for 24 hours. On the Mac, run:
  studio agent pair wss://relay.example.com/agent K7QM-2XRD-9VHP
```

Enter the relay URL and the code in Spec Studio > Settings (⌘,) > Remote Relay, or run the `studio agent pair` command. The code works once. For a new code, run `relay pair-code` where the relay runs (with the JSON store, on the same data folder):

```sh
docker compose exec relay relay pair-code
kubectl exec deploy/relay -- relay pair-code
```

Pairing is the only way to get an agent secret. The relay keeps only its SHA-256. Unpair in the Settings pane, or with `studio agent unpair`.

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

## The Temporal Web UI (Temporal store)

The payloads in Temporal are encrypted. The Web UI can show the payloads of the spec workflows through the codec endpoints of the relay:

1. Set `UI_ORIGIN` to the origin of the Web UI, for example `https://cloud.temporal.io` or `http://localhost:8233`.
2. Open `https://relay.example.com/codec/signin` and sign in with the owner password. The sign-in lasts 12 hours.
3. In the Web UI, set the codec endpoint to `https://relay.example.com/codec` and turn on "Include cross-origin credentials".

The codec endpoints load only the data key. The payloads of the relay-config workflow (the hashes of the sign-ins and the agent secrets) stay encrypted. A relay on the JSON store has no codec endpoints (404).

## Settings

| Setting | Required | Description |
|---|---|---|
| `PUBLIC_URL` | yes | The public origin of the relay. It is the OAuth issuer. |
| `OWNER_PASSWORD_HASH` | yes | The bcrypt hash of the owner password. |
| `RELAY_STORE` | no | `json` or `temporal`. Default: `temporal` when `TEMPORAL_ADDRESS` is set, else `json`. |
| `RELAY_DATA_DIR` | no | The folder of the JSON store. Default `/data`. |
| `PAYLOAD_KEYS` | Temporal | The codec keys, from `relay keys`. See below. |
| `TEMPORAL_ADDRESS` | Temporal | `host:port` of Temporal. |
| `TEMPORAL_NAMESPACE` | no | The namespace. Default `default`. |
| `TEMPORAL_AUTH` | no | `plain`, `tls`, `mtls` or `api-key`. Default: from the other Temporal settings, else `plain`. |
| `TEMPORAL_API_KEY` | no | The API key (Temporal Cloud). |
| `TEMPORAL_TLS_CERT`, `TEMPORAL_TLS_KEY` | no | The client certificate and key for mTLS: PEM text, or the path of a PEM file. |
| `DEPLOY_ID` | no | The name of this deploy. Default `default`. Two relays on one namespace need two names. |
| `TASK_QUEUE` | no | The task queue of the worker. Default `relay-<DEPLOY_ID>`. The relay-config workflow uses `<TASK_QUEUE>-config`. |
| `BUCKET_ENDPOINT`, `BUCKET_NAME`, `BUCKET_ACCESS_KEY`, `BUCKET_SECRET_KEY` | no | The attachment bucket. Set all four, or none. |
| `BUCKET_REGION` | no | The bucket region. Default `auto` (R2). |
| `ATTACHMENT_MAX_BYTES` | no | The largest attachment. Default 10485760 (10 MB). |
| `UI_ORIGIN` | no | The origin of the Temporal Web UI for the codec endpoints (Temporal store). |
| `EXTRA_REDIRECT_URIS` | no | Comma-separated OAuth redirect URIs, besides the claude.ai and claude.com callbacks and loopback addresses. |
| `PORT` | no | The HTTP port. Default 8760. |
| `CALL_TIMEOUT_MS` | no | How long a tool call waits for the Mac. Default 90000. |

The settings marked "Temporal" are required only with the Temporal store; the JSON store ignores the Temporal settings. A setting that is missing or not valid stops the start. The message names the setting and never shows its value.

### PAYLOAD_KEYS

A comma-separated list of `<scope>:<key id>:<base64 key>`. The scope is `data` (the spec workflows) or `config` (the relay-config workflow). Each key is 32 random bytes. The first key of each scope encrypts. All keys of a scope decrypt, so you can change a key: put the new key first and keep the old key after it.

```
PAYLOAD_KEYS=data:data-2:<new>,data:data-1:<old>,config:config-1:<key>
```

## What the relay keeps

Both stores keep the same data, with the same rules (the conformance tests run each rule on both stores):

- For each spec: the last state of the spec, the turn, the last 20 activity events, the messages that wait for the Mac and the messages for Claude AI. `repoKey` is the first 12 hex characters of the SHA-256 of the home repo path. When the spec is merged or dropped and no message waits, it stays for one more hour after its last update (so the attachment links in a read stay valid and late updates still apply). Then the relay removes the spec's attachments and its data; while it does that, it refuses new updates with a clear error. A sync frame that comes after that gets `synced` and starts nothing, except a first sync (`"first":true`, or a frame that starts at line 1): that is a new spec with the same slug, so it starts again. A first sync that comes while a finished spec is still kept starts it again as a new spec (the messages that waited for the Mac go back to Claude AI as errors) when the spec was dropped or the frame's state has another `createdAt`; the same merged spec sent again does not reset. While the relay removes the data, a first sync gets `synced` with an error, so the agent sends it again. A merged spec in its grace period still takes new states; a dropped one does not.
- For the deploy: the hashes of the agent secrets, of the pairing code and of the OAuth tokens, the OAuth clients, and the repos from the last connection of the Mac.

A restart loses no message and no sign-in.

### In the JSON store

- `config.json`: the deploy's data. It holds only hashes of secrets and tokens.
- `pair-code.json`: the last pairing code from `relay pair-code` (a hash).
- `specs/<hash>.json`: one file per spec. The name is a hash of the deploy ID, the repo key and the slug, so a slug never makes a path.
- `closed.json`: the specs that the relay removed in the last 30 days, so `list_specs` with `all` still shows them and a late line starts nothing.

Each write goes to a temporary file, and a rename replaces the old file, so a crash never leaves half a file. Only the relay process writes `config.json` and the spec files, and its writes of one file never interleave. `relay pair-code` writes only `pair-code.json`, each time with a new ID; the relay takes that code as its pairing code at its next call. The files are not encrypted: protect the volume as you protect the relay. A spec file that the relay cannot read is logged (`spec_file_unreadable`) and moved aside as `<name>.unreadable`. Until the file is back, the relay refuses that spec's later lines with an error, so the Mac keeps them and sends them again; none is lost. To get it back, correct the file, rename it to its old name and restart the relay. Or run `studio agent unpair --force` on the Mac and pair it again: with no cursors, the Mac sends a first sync of each spec. The unpair removes the cursors of all specs, so a spec in a repo with sync off, or in a repo that is not mounted, then sends no gone frame when you remove it later. A spec file that the relay cannot read (for example, a file that another user owns) is logged (`spec_file_read_failed`) and stays in place; the relay refuses that spec's later lines the same way, and reads the file again at its next start.

### In the Temporal store

- One workflow per spec, with the ID `spec-<deployId>-<repoKey>-<slug>`. `deployId` is `DEPLOY_ID`, so two relays on one namespace never share a spec workflow. The retention period of the namespace removes the history of a completed spec workflow.
- One workflow per deploy, `relay-config-<DEPLOY_ID>`.

## The relay command

```
relay [serve]     Run the relay.
relay pair-code   Make a new pairing code. The old code stops working. With the Temporal
                  store the relay must run; with the JSON store, use the same RELAY_DATA_DIR.
relay keys        Print a new PAYLOAD_KEYS value (Temporal store).
relay password-hash
                  Read the owner password on stdin and print its bcrypt hash.
```

## Development

```sh
npm ci
npm test
```

The tests run each store. The rules of a spec and of the deploy's data are in `src/workflows/specRules.ts` and `src/workflows/configRules.ts`: the Temporal workflows and the JSON store both call them. `test/conformance.ts` runs the same scenarios on both stores. `test/replay.test.ts` replays workflow histories of relay 0.3, so a change of the workflow code that breaks a running workflow fails the tests.

The Temporal tests run a Temporal dev server. They use the `temporal` CLI when `TEMPORAL_CLI` names it or when it is at `/opt/homebrew/bin/temporal`. Else the Temporal SDK downloads the dev server once.

The wire contract between the relay and the Spec Studio agent (version 3) is in the Spec Studio repo, in `specs/studio-relay/wire.md`. Relay 0.3.0 and later speak version 3 only: they need Spec Studio v0.1.1 or later, and an older app gets tool calls only. The agent sends the new activity lines of a spec from one drain in one `sync` frame (`"lines":[{"line":<n>,"event":{...}},...]`), with the state and the turn only when they changed; the store applies the frame as one update and answers `synced` with the last line. A spec with no cursor on the Mac gets a short first sync (`"first":true`): its last 20 lines and its earlier messages. The agent sends `{"type":"gone","spec":{"repoKey":"<repoKey>","slug":"<slug>"}}` when a spec that it synced no longer exists on the Mac. The relay marks the spec dropped (list_specs shows it only with `all`, send_message refuses it, and the messages that waited for the Mac go back to Claude AI as errors), ends it like a merged spec, and answers `{"type":"gone-ok","spec":{"repoKey":"<repoKey>","slug":"<slug>"}}`, so the agent can drop its cursor. A `gone-ok` with an `error` field means: send `gone` again later. The agent sends `{"type":"state","spec":{"repoKey":"<repoKey>","slug":"<slug>"},"state":{...},"turn":{"turn":"...","needsYou":true}|null}` when the state (`studio show --json`) or the turn of a spec changes with no new activity line. The relay replaces the mirror's state and turn (and, in the Temporal store, the search attributes NeedsYou, Status, Stage, Mode and SpecTitle), after the earlier `sync` frames of that spec. It keeps the line cursor and the events, sets the time of the copy (`syncedAt`) to the time of the frame, and ignores the frame for a spec that the store does not have or a dropped one. There is no answer frame. When the store fails, the relay keeps the newest state frame of the spec and tries it again every 30 seconds, until it applies, a newer `state` frame or a `sync` frame with a state comes for the spec, or the agent disconnects. After the welcome, and then every 60 seconds while the agent is connected, the relay looks for messages that wait for the Mac and sends each one once per connection (the Temporal search attributes are eventually consistent, so one look can miss a new message).
