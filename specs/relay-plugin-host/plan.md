# Plan: The relay as a plugin host

## Approach

The relay gets a third implementation of its store interfaces (`SpecStore`, `DeskStore`, `RelayConfigStore` in `src/store/types.ts`): the plugin store in `src/store/plugin/`. It talks to a plugin through the plugin-sdk TypeScript host client and follows PROTOCOL.md section 14 exactly: the documents `specs`, `closed-specs`, `desks`, `relay/config`, and the streams `spec:<repoKey>:<slug>` and `desk:<repoKey>`. Each change is a "read, apply, write": `state.get`, the rule from `src/workflows/*Rules.ts`, `state.put` with the read version, and on `conflict` again, at most 5 times. So the rules stay in one place, and the JSON store, the Temporal store and the plugin store cannot drift.

**The client.** The npm package `@spec-studio-app/plugin-sdk` is not published. The relay copies `ts/src/*.ts` of plugin-sdk at commit `394b06f` (the merged protocol) into `src/vendor/plugin-sdk/`, with a `SOURCE.md` that names the commit. Reason: a git dependency cannot point at the `ts/` folder, and a copy builds with no network and no Go. The follow-up spec replaces the copy with the npm package. The copy has no change, so a later diff against plugin-sdk shows nothing.

**Settings.** `src/config.ts` gets `plugin: PluginSettings | null`: `{ transport: "stdio", command, args }` from `RELAY_PLUGIN` (split into words as a shell does, with quotes, but no other shell feature), or `{ transport: "http", url, token }` from `RELAY_PLUGIN_URL` and `RELAY_PLUGIN_TOKEN`. Each other `RELAY_PLUGIN_<NAME>` becomes the handshake setting `<name>` in lowercase. `store` gets a third value `plugin`, and `storeReason` the value `plugin`. Both plugin settings, or a plugin with `RELAY_STORE`, is a `ConfigError` that names the settings. With no plugin setting, nothing changes (R1.S1).

**Start.** `openPluginStore` connects (`PluginClient.startStdio` or `connectHttp`), and the client does the handshake with `host: { kind: "relay", version }`. A failed start or handshake becomes a `StartupError` with the plugin's reason and what to check; `cli.ts` already prints a `StartupError` and exits with 1. Then one log line `plugin_ready` with name, version, repo and capabilities (never the settings). Then the first config with `state.put` and `expectedVersion: null`; a `conflict` means that a config exists. Then the host timers: `state.list` on `specs` (filter `deployId`, `open: false`) gives the finished specs, and each gets its grace timer from the `lastActivityAt` in its document. So the grace period survives a restart (R3.S5).

**Waits.** `read` uses `watch.wait` on the document with its version, and waits again after each write with no new message, until `waitMs` ends. A plugin with no `watch` polls each second. A deleted document (the spec closed) ends the read.

**Closing.** The plugin store has the JSON store's grace logic: a timer per finished spec, then remove the attachments (bucket), `state.put` on `closed-specs`, `state.delete` on `specs`, `events.delete` on the stream. A sync with no `specs` document reads `closed-specs` first, as PROTOCOL.md says. Closed records older than 30 days go at start and at each close.

**Errors.** A `PluginError` with `unavailable` (the plugin is down, or 5 conflicts) becomes a new `StoreUnavailableError` in `src/store/errors.ts` with the text "The relay's store is not available. Try again in a minute." The tool handlers in `src/messages.ts`, `src/offline.ts` and the agent hub answer it with that text. A `refused` (for example over the size limit) becomes an `Error` with the plugin's text. The TypeScript client already restarts a stdio plugin with the backoff of PROTOCOL.md and logs the exit code and stderr (R4.S1); the relay passes its logger.

**Pairing codes from another process.** `relay pair-code` opens the plugin store with `worker: false`: it connects, does a read, apply, write on `relay/config`, and closes. With an HTTP sidecar this works next to the running relay. A stdio plugin whose storage another process holds answers `unavailable`; the command then prints what to do.

**Close.** A stdio plugin gets `plugin.shutdown` (the client's `shutdown()`); an HTTP sidecar gets no call (`close()` only).

**Deploy files.** Compose gets the three settings, empty, and a commented sidecar service. The chart gets `plugin.command`, `plugin.url`, `plugin.settings` and `plugin.sidecar` (image, args, a token in the secret). With default values, `helm template` is the same as before.

**Version 0.7.0.** `main` is at 0.5.0, and a 0.6.0 release is in progress on `release/0.6.0`. So this change takes 0.7.0. The release notes go in `release-notes/0.7.0.md`, and `release.yml` adds the file of the version to the GitHub release when it exists.

## Repos
| Repo | Why it changes | Merge order |
|---|---|---|
| spec-studio-relay | The plugin store, settings, tests, deploy files, version | 1 |
| spec-studio-site | The relay guide and the backends guide say that the store can be a plugin | 2 |

## Steps
1. spec-studio-relay: copy the plugin-sdk TS client into `src/vendor/plugin-sdk/` with `SOURCE.md`; build passes. Covers R2.
2. spec-studio-relay: the plugin settings in `src/config.ts` (`RELAY_PLUGIN`, `RELAY_PLUGIN_URL`, `RELAY_PLUGIN_TOKEN`, `RELAY_PLUGIN_<NAME>`, the conflicts, the command split), and `storeReasonText` in `src/store/index.ts`; unit tests in `test/config.test.ts`. Covers R1.
3. spec-studio-relay: `StoreUnavailableError` in `src/store/errors.ts`, and its text in the tool handlers (`src/messages.ts`, `src/offline.ts`, the agent hub). Covers R4.
4. spec-studio-relay: the plugin store in `src/store/plugin/` (`docs.ts` for read, apply, write and error mapping; `pluginConfigStore.ts`; `pluginSpecStore.ts` with waits, events and closing; `pluginDeskStore.ts`; `index.ts` with `openPluginStore`, the handshake, the log line, the first config, the timers and the close), and `openStore` picks it. Covers R2, R3, R4.
5. spec-studio-relay: test support: `test/plugin.ts` finds or builds the memory plugin (`RELAY_TEST_PLUGIN`, else `go build` from `PLUGIN_SDK_DIR`, else skip with a note), and a fake plugin script `test/fixtures/fakePlugin.mjs` for the handshake failures, the version mismatch, conflicts and exits. Covers R3, R4.
6. spec-studio-relay: tests: `test/conformance.plugin.test.ts` (the conformance suite over stdio and HTTP), `test/pluginStore.test.ts` (desks, waits, conflicts, closing, the restart, pair-code, the log line, shutdown), and the CI job in `.github/workflows/test.yml` that checks out plugin-sdk at the pinned commit, sets up Go and builds the memory plugin. Covers R2, R3, R4.
7. spec-studio-relay: `compose.yaml` and the Helm chart (`values.yaml`, `templates/deployment.yaml`, `templates/secret.yaml`), with tests in `test/packaging.test.ts`. Covers R5.
8. spec-studio-relay: README (the store section and the settings), version 0.7.0 in `package.json`, `package-lock.json`, `chart/relay/Chart.yaml` and `test/packaging.test.ts`, `release-notes/0.7.0.md`, and `release.yml` that adds it. Covers R6.
9. spec-studio-site: `src/guides/backends.md` gets a section "Store plugins", `src/guides/relay.md` step 2 names it, the settings table gets the new settings; a test in `tests/backends.spec.ts`. Covers R6.

## Tests
- R1.S1: `test/config.test.ts`: with no plugin setting, `loadConfig` gives the same `store` and `storeReason` as before; the JSON and Temporal conformance suites still pass unchanged.
- R1.S2: `test/config.test.ts`: `RELAY_PLUGIN` with arguments and quotes gives the command and args; `test/pluginStore.test.ts` starts the memory plugin from `RELAY_PLUGIN` through `openStore`.
- R1.S3: `test/conformance.plugin.test.ts` runs the suite against the memory plugin with `--listen` and `--token`; a wrong token stops the start.
- R1.S4: `test/config.test.ts`: the settings map; `test/pluginStore.test.ts`: the fake plugin echoes its handshake settings.
- R1.S5: `test/config.test.ts`: both plugin settings, and a plugin with `RELAY_STORE`, throw a `ConfigError` that names them and holds no value.
- R2.S1: `test/pluginStore.test.ts`: the log has one `plugin_ready` entry with name, version, repo and capabilities, and no secret value.
- R2.S2: `test/pluginStore.test.ts`: the fake plugin refuses the handshake with `bad_settings`, and a command that does not exist: `start` throws a `StartupError` with the reason and the hint.
- R2.S3: `test/pluginStore.test.ts`: the fake plugin speaks only 2.0: the `StartupError` names both lists of versions.
- R2.S4: `test/pluginStore.test.ts`: the first start prints a pairing code and returns it; a second start on the same data file prints none; the log has no code.
- R3.S1: `test/conformance.plugin.test.ts` (stdio and HTTP), and the desk scenarios in `test/pluginStore.test.ts`.
- R3.S2: `test/pluginStore.test.ts`: parallel sends on one spec all get their own seq; the fake plugin that always answers `conflict` makes the call fail with `StoreUnavailableError` after 5 tries.
- R3.S3: `test/pluginStore.test.ts`: a read with `waitMs` 10 s returns the reply that a sync adds after 300 ms, in less than 2 s.
- R3.S4: `test/pluginStore.test.ts`: a merged spec with a short grace closes: the attachments go, `state` gives null, `list({ all: true })` shows it with `running: false`, and a late line is not applied.
- R3.S5: `test/pluginStore.test.ts`: close the store before the grace period ends, open a new one on the same memory data file: the spec closes.
- R3.S6: `test/pluginStore.test.ts`: a relay store and a second store (as `relay pair-code`) on one HTTP memory plugin: the new code pairs, the old code does not.
- R4.S1: `test/pluginStore.test.ts`: the fake plugin exits on a signal file; the log has the exit code and stderr, and the plugin comes back with a handshake.
- R4.S2: `test/pluginStore.test.ts`: while the fake plugin is down, `specs.state` throws `StoreUnavailableError`; `test/messages.test.ts`: the tool answers its text; after the restart, the call works.
- R4.S3: `test/pluginStore.test.ts`: `close()` on a stdio store makes the plugin exit with code 0; on an HTTP store, the plugin gets no `plugin.shutdown` (the fake plugin records its calls).
- R5.S1: `test/packaging.test.ts`: Compose has the three settings with empty defaults and a commented sidecar example.
- R5.S2: `test/packaging.test.ts`: `helm template` with defaults has no plugin setting and one container; with `plugin.sidecar` values, two containers, `RELAY_PLUGIN_URL` and the token from the secret.
- R6.S1: `tests/backends.spec.ts` in the site: the store plugins section names JSON as the default, links plugin-json and plugin-temporal, and shows `RELAY_PLUGIN` and `RELAY_PLUGIN_URL`.
- R6.S2: `test/packaging.test.ts`: version 0.7.0 in the package, the lock and the chart; `release-notes/0.7.0.md` says that nothing changes for owners.

## Risks
- The copied client can drift from plugin-sdk. `SOURCE.md` names the commit, and the follow-up spec moves to the npm package.
- The tests need Go to build the memory plugin. CI sets up Go; locally, the plugin tests skip with a clear note when Go or plugin-sdk is missing, so `npm test` still works.
- The release/0.6.0 work and this change both touch the version lines. This change takes 0.7.0, so a merge conflict is only in the version lines.
- A plugin store reads a document for each call, so it is slower than the in-memory JSON store. A relay has one Mac and few specs, so this is acceptable; the JSON store stays the default.
