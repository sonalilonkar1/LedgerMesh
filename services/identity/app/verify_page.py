"""The page a sign-in link opens (LM-052).

The emailed link is ``<IDENTITY_LOGIN_LINK_URL>#token=<token>``. Browsers never
send the fragment to a server, so the token stays out of access logs, proxies
and Referer headers. Opening the link only loads this static page; the token is
consumed when the person presses the button, so mail scanners and link
prefetchers that fetch the URL cannot use up the link.

The script and style are inline and allowed by their hashes in the
Content-Security-Policy, so the page loads nothing else.
"""

import base64
import hashlib

STYLE = (
    "body{font-family:system-ui,sans-serif;max-width:32rem;margin:4rem auto;padding:0 1rem;"
    "line-height:1.5;color:#1f2933}button{font:inherit;padding:.6rem 1.2rem;border-radius:.4rem;"
    "border:0;background:#1d4ed8;color:#fff;cursor:pointer}button[disabled]{opacity:.6}"
)

SCRIPT = """
(function () {
  var token = new URLSearchParams(window.location.hash.slice(1)).get("token");
  history.replaceState(null, "", window.location.pathname);
  var button = document.getElementById("confirm");
  var status = document.getElementById("status");
  if (!token) {
    status.textContent = "This sign-in link is incomplete. Request a new one.";
    return;
  }
  status.textContent = "Press the button to finish signing in.";
  button.hidden = false;
  button.addEventListener("click", function () {
    button.disabled = true;
    status.textContent = "Signing in...";
    fetch(window.location.pathname, {
      method: "POST",
      credentials: "same-origin",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({token: token})
    }).then(function (response) {
      return response.json().then(function (body) {
        return {ok: response.ok, body: body};
      });
    }).then(function (result) {
      button.hidden = true;
      status.textContent = result.ok
        ? "You are signed in as " + result.body.user.email + ". You can close this tab."
        : (result.body.detail || "Sign-in failed. Request a new link.");
    }).catch(function () {
      button.disabled = false;
      status.textContent = "Network error. Try again.";
    });
  });
})();
"""


def _csp_hash(source: str) -> str:
    return "'sha256-" + base64.b64encode(hashlib.sha256(source.encode()).digest()).decode() + "'"


CONTENT_SECURITY_POLICY = (
    "default-src 'none'; "
    f"script-src {_csp_hash(SCRIPT)}; "
    f"style-src {_csp_hash(STYLE)}; "
    "connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
)

PAGE_HEADERS = {
    "Content-Security-Policy": CONTENT_SECURITY_POLICY,
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
    "Cache-Control": "no-store",
}

PAGE = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="referrer" content="no-referrer">
<title>Sign in to LedgerMesh</title>
<style>{STYLE}</style>
</head>
<body>
<h1>Sign in to LedgerMesh</h1>
<p id="status">Enable JavaScript to finish signing in.</p>
<button id="confirm" type="button" hidden>Sign in to LedgerMesh</button>
<script>{SCRIPT}</script>
</body>
</html>
"""
