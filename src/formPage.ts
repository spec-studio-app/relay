// The sign-in pages (OAuth login and codec sign-in) share one strict CSP and
// one small script: after the first submit the button turns off and says
// "Signing in…", so an impatient second click or a password manager's
// auto-submit cannot send the form twice.
import { randomBytes } from "node:crypto";
import type { Response } from "express";

/** A new nonce for one page. */
export function newNonce(): string {
  return randomBytes(16).toString("base64");
}

/** The CSP of a sign-in page: no sources except inline styles and the one nonced script. */
export function formPageCsp(nonce: string): string {
  return `default-src 'none'; style-src 'unsafe-inline'; script-src 'nonce-${nonce}'; form-action 'self'; frame-ancestors 'none'`;
}

export function setFormPageHeaders(res: Response, nonce: string): void {
  res.setHeader("Cache-Control", "no-store");
  res.setHeader("Content-Security-Policy", formPageCsp(nonce));
}

/** Extra CSS for the busy button. */
export const SUBMITTING_STYLE = "button[disabled]{opacity:.6;cursor:progress}";

/** The script that gives the form its submitting state. */
export function submittingScript(nonce: string): string {
  return `<script nonce="${nonce}">
(function () {
  var form = document.querySelector("form");
  if (!form) return;
  var sent = false;
  form.addEventListener("submit", function (event) {
    if (sent) { event.preventDefault(); return; }
    sent = true;
    var button = form.querySelector("button[type=submit]");
    if (button) {
      button.disabled = true;
      button.textContent = "Signing in…";
      button.setAttribute("aria-busy", "true");
    }
  });
  // Back/forward cache: a restored page can submit again.
  window.addEventListener("pageshow", function (event) {
    if (!event.persisted) return;
    sent = false;
    var button = form.querySelector("button[type=submit]");
    if (button) { button.disabled = false; button.textContent = "Sign in"; button.removeAttribute("aria-busy"); }
  });
})();
</script>`;
}
