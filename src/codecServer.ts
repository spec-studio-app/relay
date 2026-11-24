// The codec endpoints for the Temporal Web UI: POST /codec/encode and
// /codec/decode. They load only the data key, so the payloads of the
// relay-config workflow stay encrypted. They need the owner's sign-in: an OAuth
// bearer token of the relay, or the session cookie from /codec/signin.
// CORS allows only UI_ORIGIN.
import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import express, { type NextFunction, type Request, type Response, type Router } from "express";
import type { Payload } from "@temporalio/common";
import { AesGcmCodec } from "./codec.js";
import type { PayloadKey } from "./config.js";
import type { SingleUserProvider } from "./oauth.js";
import type { Logger } from "./log.js";
import { newNonce, setFormPageHeaders, submittingScript, SUBMITTING_STYLE } from "./formPage.js";

export const SESSION_COOKIE = "relay_session";
const SESSION_TTL_MS = 12 * 60 * 60 * 1000;

interface JsonPayload {
  metadata?: Record<string, string>;
  data?: string;
}

function fromJson(p: JsonPayload): Payload {
  const metadata: Record<string, Uint8Array> = {};
  for (const [k, v] of Object.entries(p.metadata ?? {})) metadata[k] = Buffer.from(v, "base64");
  return { metadata, data: Buffer.from(p.data ?? "", "base64") };
}

function toJson(p: Payload): JsonPayload {
  const metadata: Record<string, string> = {};
  for (const [k, v] of Object.entries(p.metadata ?? {})) metadata[k] = Buffer.from(v ?? new Uint8Array()).toString("base64");
  return { metadata, data: Buffer.from(p.data ?? new Uint8Array()).toString("base64") };
}

export interface CodecServerOptions {
  dataKeys: PayloadKey[];
  uiOrigin?: string;
  provider: SingleUserProvider;
  /** The owner's password hash: it signs the session cookie, so a new password ends old sessions. */
  passwordHash: string;
  log: Logger;
}

export function codecRouter(o: CodecServerOptions): Router {
  const router = express.Router();
  // The Web UI also sends payloads that are not encrypted (or use the config
  // key): they pass unchanged, for display only.
  const codec = new AesGcmCodec({ encryptKey: o.dataKeys[0], decryptKeys: o.dataKeys, unknownKey: "keep", plain: "keep" });
  const sessionKey = createHash("sha256").update("relay codec session\0").update(o.passwordHash).digest();
  const sign = (value: string) => createHmac("sha256", sessionKey).update(value).digest("base64url");

  const sessionValid = (req: Request): boolean => {
    for (const part of (req.headers.cookie ?? "").split(";")) {
      const [name, ...rest] = part.trim().split("=");
      if (name !== SESSION_COOKIE) continue;
      const [expires, signature] = rest.join("=").split(".");
      if (!expires || !signature) continue;
      const expected = Buffer.from(sign(expires));
      const actual = Buffer.from(signature);
      if (expected.length === actual.length && timingSafeEqual(expected, actual) && Number(expires) > Date.now()) return true;
    }
    return false;
  };

  const cors = (req: Request, res: Response, next: NextFunction) => {
    const origin = req.headers.origin;
    if (o.uiOrigin && origin === o.uiOrigin) {
      res.setHeader("Access-Control-Allow-Origin", origin);
      res.setHeader("Access-Control-Allow-Credentials", "true");
      res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
      res.setHeader("Access-Control-Allow-Headers", "content-type, x-namespace, authorization");
      res.setHeader("Vary", "Origin");
    }
    if (req.method === "OPTIONS") {
      res.status(204).end();
      return;
    }
    next();
  };

  const signedIn = async (req: Request, res: Response, next: NextFunction) => {
    if (sessionValid(req)) return next();
    const match = /^Bearer\s+(.+)$/i.exec(req.headers.authorization ?? "");
    if (match) {
      try {
        await o.provider.verifyAccessToken(match[1]);
        return next();
      } catch {
        // fall through
      }
    }
    o.log({ event: "codec_rejected", status: 401 });
    res.status(401).json({ error: "Sign in to the relay first: open /codec/signin on the relay." });
  };

  const handler = (direction: "encode" | "decode") => async (req: Request, res: Response) => {
    const payloads = Array.isArray(req.body?.payloads) ? (req.body.payloads as JsonPayload[]) : null;
    if (!payloads) {
      res.status(400).json({ error: "The body must be {\"payloads\": [...]}." });
      return;
    }
    try {
      const input = payloads.map(fromJson);
      const out = direction === "encode" ? await codec.encode(input) : await codec.decode(input);
      res.json({ payloads: out.map(toJson) });
    } catch {
      o.log({ event: "codec_failed", direction });
      res.status(400).json({ error: "The relay cannot read a payload." });
    }
  };

  for (const direction of ["encode", "decode"] as const) {
    router.options(`/codec/${direction}`, cors);
    router.post(`/codec/${direction}`, cors, signedIn, express.json({ limit: "8mb" }), handler(direction));
  }

  router.get("/codec/signin", (_req, res) => sendSignIn(res));
  router.post("/codec/signin", express.urlencoded({ extended: false, limit: "4kb" }), async (req: Request, res: Response) => {
    const password = typeof req.body?.password === "string" ? req.body.password : "";
    const result = await o.provider.checkPassword(req, password);
    if (result !== "ok") {
      o.log({ event: "codec_signin_failed", status: result === "locked" ? 429 : 401 });
      res.status(result === "locked" ? 429 : 401);
      sendSignIn(res, result === "locked" ? "Too many wrong passwords. Wait 15 minutes, then try again." : "The password is not correct.");
      return;
    }
    o.provider.rememberDevice(req, res);
    const expires = String(Date.now() + SESSION_TTL_MS);
    // SameSite=None: the Web UI calls the codec from its own origin with credentials.
    res.cookie(SESSION_COOKIE, `${expires}.${sign(expires)}`, {
      httpOnly: true,
      secure: true,
      sameSite: "none",
      path: "/codec",
      maxAge: SESSION_TTL_MS,
    });
    o.log({ event: "codec_signin_ok" });
    res.setHeader("Cache-Control", "no-store");
    res.type("html").send(page("<h1>Signed in</h1><p>The Temporal Web UI can now show the payloads of the spec workflows for 12 hours. Go back to the Web UI.</p>"));
  });

  return router;
}

function page(body: string): string {
  return `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Relay codec sign-in</title>
<style>:root{color-scheme:light dark}body{font:16px/1.4 -apple-system,system-ui,sans-serif;display:grid;place-items:center;min-height:100vh;margin:0;padding:0 16px}
form{width:100%;max-width:320px;display:grid;gap:12px}h1{font-size:20px;margin:0}input,button{font:inherit;padding:10px 12px;border-radius:8px;border:1px solid #8888}.error{color:#d33;margin:0}${SUBMITTING_STYLE}</style>
</head><body>${body}</body></html>`;
}

function sendSignIn(res: Response, error?: string): void {
  const nonce = newNonce();
  setFormPageHeaders(res, nonce);
  const escaped = error?.replace(/[&<>"']/g, (c) => `&#${c.charCodeAt(0)};`);
  res.type("html").send(
    page(`<form method="post" action="/codec/signin">
<h1>Spec Studio relay</h1>
<p>Sign in, so that the Temporal Web UI can show the payloads of the spec workflows.</p>
${escaped ? `<p class="error">${escaped}</p>` : ""}
<input type="password" name="password" autocomplete="current-password" placeholder="Password" required autofocus>
<button type="submit">Sign in</button>
</form>
${submittingScript(nonce)}`),
  );
}
