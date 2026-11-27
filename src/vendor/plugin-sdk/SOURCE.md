# The plugin-sdk TypeScript host client (a copy)

These files are a copy of `ts/src/` in [spec-studio-app/plugin-sdk](https://github.com/spec-studio-app/plugin-sdk), at commit `394b06fac3b99e4255f69453ffe51eadf7b94d19` (the merged protocol 1.0).

The npm package `@spec-studio-app/plugin-sdk` is not published yet. When it is, the relay depends on it and this folder goes. Do not change these files here: change them in plugin-sdk, then copy them again.

One change is here before plugin-sdk has it: the optional `storeId` in `HandshakeResult` (`types.ts`, protocol 1.1, contract 2 of `specs/plugins-are-the-backend` in the Spec Studio repo). When plugin-sdk has it, copy the files again.
