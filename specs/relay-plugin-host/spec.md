# The relay as a plugin host

The relay keeps its state in two built-in stores: JSON files and Temporal. This change makes the relay a host of backend plugins, as `PROTOCOL.md` in spec-studio-app/plugin-sdk defines. The owner can run any plugin over stdio (`RELAY_PLUGIN`) or over HTTP as a sidecar (`RELAY_PLUGIN_URL`). The relay keeps all the rules, and the plugin only stores. The official plugins (plugin-json, plugin-temporal) are not released yet, so the built-in stores stay the default until a follow-up spec switches to them.

## Original request

Make the relay a plugin host: its JSON and Temporal stores move out into backend plugins, with no change in what users see. (Full text in the first version of this file, in git.)

## Decision on the dependencies

On 2026-11-27, plugin-json and plugin-temporal had no code and no release, and the plugin-sdk npm package was not published. The user chose: build the plugin host now; keep the built-in JSON and Temporal stores behind the same interface; a follow-up spec switches `RELAY_STORE=json` and `RELAY_STORE=temporal` to the plugin binaries and puts the pinned binaries in the image.

## Scope

### In scope
- A plugin store in the relay: it implements `SpecStore`, `DeskStore` and `RelayConfigStore` over the protocol, as PROTOCOL.md section 14 says. The rules in `src/workflows/*Rules.ts` run in the relay.
- The settings `RELAY_PLUGIN`, `RELAY_PLUGIN_URL`, `RELAY_PLUGIN_TOKEN` and the pass-through of `RELAY_PLUGIN_<NAME>` settings.
- The start: the handshake, the log line with the plugin's name, version, repo link and capabilities, and a clear stop when the handshake fails.
- The restart of a stdio plugin with backoff, and the answer "The relay's store is not available" while the plugin is down.
- The relay commands (`relay pair-code` and the others that open the store) with a plugin.
- The store tests of the relay run against the plugin store, with the plugin-sdk memory plugin, over stdio and over HTTP, in CI.
- The Helm chart and the Docker Compose file: the new settings, and an example of a sidecar plugin.
- The relay guide on the site: the store can be a plugin, the two official plugins with links, how to use another plugin. The default (JSON) stays clear.
- A minor version bump of the relay, with release notes that say nothing changes for owners.

### Out of scope
- The plugin-json and plugin-temporal binaries in the image, pinned by version and SHA-256. A follow-up spec does this when the plugins are released.
- A switch of `RELAY_STORE=json` or `RELAY_STORE=temporal` to a plugin. The built-in stores stay.
- The test job that runs the relay's store tests against each official plugin binary. The follow-up spec adds it.
- The codec endpoints with a plugin store. They keep working with the built-in Temporal store, as today.
- Attachments in the plugin's `blobs`. Attachments stay in the bucket, as today.
- History and replay of a spec (`events` reads) in the relay's tools.

## Requirements

### R1 Choose a plugin with the settings
The owner chooses a plugin with `RELAY_PLUGIN` (a command for stdio) or `RELAY_PLUGIN_URL` (an HTTP URL). With neither set, the relay chooses its store as today.

#### R1.S1 No plugin setting keeps today's store
- Given a relay with no `RELAY_PLUGIN` and no `RELAY_PLUGIN_URL`
- When the relay starts
- Then it uses the JSON store, or the Temporal store when `RELAY_STORE=temporal` or `TEMPORAL_ADDRESS` is set, as before this change
- And the existing data folder or Temporal namespace works with no step from the owner

#### R1.S2 A stdio plugin from a command
- Given `RELAY_PLUGIN` is set to a command with arguments, for example `/usr/local/bin/memory --data /data/memory.json`
- When the relay starts
- Then the relay starts that command as a child process and uses it as its store over stdio

#### R1.S3 An HTTP sidecar plugin from a URL
- Given `RELAY_PLUGIN_URL` is set to the plugin's `/rpc` URL, and `RELAY_PLUGIN_TOKEN` holds its token
- When the relay starts
- Then the relay calls the plugin over HTTP with the token as a bearer token

#### R1.S4 Settings pass through with a prefix
- Given `RELAY_PLUGIN_ADDRESS=temporal:7233` and `RELAY_PLUGIN_API_KEY=secret` are set
- When the relay does the handshake
- Then the handshake settings are `address` and `api_key` with these values
- And `RELAY_PLUGIN_URL` and `RELAY_PLUGIN_TOKEN` are not passed as settings

#### R1.S5 Settings that conflict stop the start
- Given both `RELAY_PLUGIN` and `RELAY_PLUGIN_URL` are set, or a plugin is set together with `RELAY_STORE`
- When the relay starts
- Then the start stops with a message that names the settings that conflict
- And the message holds no setting value

### R2 The handshake at start
At start, the relay does the handshake with the plugin and logs what the plugin is. A failed handshake stops the start with a message for the owner.

#### R2.S1 The log names the plugin
- Given a plugin that answers the handshake
- When the relay starts
- Then the log has one line with the plugin's name, version, repo link and capabilities
- And the log has no value of a secret setting

#### R2.S2 A failed handshake stops the start
- Given a plugin that refuses the handshake (for example a missing required setting) or that does not start
- When the relay starts
- Then the relay stops with exit code 1
- And the message says that the plugin failed, gives the plugin's own reason, and says what to check (the command, the URL, the token or the `RELAY_PLUGIN_` settings)

#### R2.S3 A plugin with no shared protocol version
- Given a plugin that speaks only protocol version 2.0
- When the relay starts
- Then the start stops with a message that names the relay's protocol versions and the plugin's versions

#### R2.S4 The first start makes the config
- Given a plugin with no relay config
- When the relay starts
- Then the relay makes the config, prints the pairing code to the console, and does not log it
- And a second start on the same storage does not make a new code

### R3 The relay works the same with a plugin
With a plugin store, every relay tool, the agent hub and OAuth give the same results as with the JSON store.

#### R3.S1 The store conformance suite passes
- Given the plugin-sdk memory plugin, over stdio and over HTTP
- When the relay's store conformance suite and desk store tests run against the plugin store
- Then every scenario passes with the same expected values as for the JSON store

#### R3.S2 Two writes to the same spec
- Given two calls that change the same spec at the same time
- When the plugin answers one write with `conflict`
- Then the relay reads the spec again, applies the rule again and writes again
- And after 5 conflicts in a row the call fails as the store not available

#### R3.S3 A long poll ends at a new message
- Given Claude AI waits in `read_messages` on a spec with no new message
- When the Mac's reply comes to the spec
- Then the wait ends and returns the reply, before `waitMs` ends

#### R3.S4 A finished spec closes after its grace period
- Given a merged spec with empty inboxes, on a plugin store
- When the grace period ends with no call to the spec
- Then the relay removes the spec's attachments and its document, and keeps a closed record
- And a list with `all` shows the spec as not running, and a late line for it is not applied

#### R3.S5 The grace period survives a restart
- Given a merged spec with empty inboxes, and the relay restarts before the grace period ends
- When the grace period ends
- Then the restarted relay closes the spec

#### R3.S6 The pairing code from another process
- Given a relay that runs on an HTTP sidecar plugin
- When the owner runs `relay pair-code` with the same settings
- Then the new code works for the next pairing, and the old code does not

### R4 A stdio plugin that exits
The relay starts a stdio plugin again when it exits, with backoff. Meanwhile, each call that needs the store fails with a clear text.

#### R4.S1 Restart with backoff
- Given a stdio plugin that exits when the relay did not ask it to
- When the relay sees the exit
- Then the log has the exit code and the last lines of stderr
- And the relay starts the plugin again after 250 ms, then twice as long each time, at most 30 s, and sends the same handshake

#### R4.S2 Tools while the plugin is down
- Given the stdio plugin is down
- When Claude AI calls a relay tool that needs the store
- Then the tool answers "The relay's store is not available" and the relay does not crash
- And after the restart, the same tool works with the data from before

#### R4.S3 The relay stops the plugin
- Given a relay with a stdio plugin
- When the relay gets SIGTERM
- Then the relay sends `plugin.shutdown` and the plugin exits with code 0
- And with an HTTP sidecar, the relay sends no `plugin.shutdown`

### R5 Deploy files
The Helm chart and the Docker Compose file take the plugin settings, and each has an example of a sidecar plugin.

#### R5.S1 Compose with a sidecar
- Given the Compose file
- When the owner reads it
- Then it has `RELAY_PLUGIN`, `RELAY_PLUGIN_URL` and `RELAY_PLUGIN_TOKEN`, empty by default
- And a commented example shows a plugin sidecar service and the settings that point the relay at it

#### R5.S2 Helm with a sidecar
- Given the Helm chart with default values
- When `helm template` runs
- Then the output is the same as before this change
- And with `plugin.sidecar` values set, the pod has a second container for the plugin and the relay has `RELAY_PLUGIN_URL` and the token from a secret

### R6 Docs and version
The relay guide on the site says that the store can be a plugin. The relay gets a minor version bump.

#### R6.S1 The guide explains plugins
- Given the relay guide on the site
- When an owner reads the store section
- Then it says that JSON is the default and needs no setting
- And it names plugin-json and plugin-temporal with links to their repos, and says that the relay will use them in a later version
- And it shows how to use another plugin with `RELAY_PLUGIN` or `RELAY_PLUGIN_URL`

#### R6.S2 The version and the release notes
- Given the relay's `package.json` and the README
- When the change merges
- Then the version is the next minor version
- And the release notes say that nothing changes for owners, and that plugin stores are new and optional

## Edge cases
- A plugin that does not give `watch`: a read polls the document every second until `waitMs` ends.
- A plugin that does not give `events`: the relay does not append events, and nothing else changes.
- A document or event larger than the plugin's size limit: the call fails with the plugin's `refused` text, and the relay does not crash.
- `relay pair-code` with a stdio plugin whose storage another process holds: the command stops with a message that says to run it with the same settings as the relay while the relay is stopped, or to use an HTTP sidecar.
- An HTTP sidecar that restarts: the client does the handshake again and retries the call once.
- `RELAY_PLUGIN` with quotes: the command is split into words as a shell does, with no other shell features (no `$`, no pipes).
- `RELAY_PLUGIN_` names: the setting name is the rest of the name in lowercase. A name that is not a valid setting name stops the start.

## Assumptions
- The built-in JSON and Temporal stores stay the default until the official plugins are released (the user's decision above).
- The relay copies the plugin-sdk TypeScript host client into its source, pinned to the merged plugin-sdk commit, because the npm package is not published. The follow-up spec moves to the npm package.
- The tests use the plugin-sdk memory plugin, built with Go from the pinned plugin-sdk commit.
- The relay version goes to the next minor version above `main` at merge time. A 0.6.0 release branch is open, so this can be 0.7.0.
- With a plugin store, the codec endpoints are off, as with the JSON store today.
- The site guide is in spec-studio-app/site. That repo changes too.
