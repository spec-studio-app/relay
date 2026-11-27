# The plugin-sdk TypeScript host client (a copy)

These files are a copy of `ts/src/` in [spec-studio-app/plugin-sdk](https://github.com/spec-studio-app/plugin-sdk), at commit `2834b2ad914418fb756190337517b094b945bc07` on the branch `spec/plugins-are-the-backend` (protocol 1.1: `state.commit`, the optional `storeId` in the handshake result, and the host kind `cli`). The later commits of that branch change only `PROTOCOL.md`, not `ts/src/`.

The client offers protocol versions `1.0` and `1.1` (`PROTOCOL_VERSIONS`). A plugin that speaks 1.1 chooses 1.1 and can give a `storeId`; a plugin that speaks only 1.0 chooses 1.0 and gives none. The relay works with both: it does not call `state.commit`.

The npm package `@spec-studio-app/plugin-sdk` is not published yet. When it is, the relay depends on it and this folder goes. Do not change these files here: change them in plugin-sdk, then copy them again.
