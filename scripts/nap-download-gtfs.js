const fs = require("node:fs");
const { Readable } = require("node:stream");
const { pipeline } = require("node:stream/promises");

const TOKEN_URL = "https://b2b.nap.si/uc/user/token";
const GTFS_URL = "https://b2b.nap.si/data/b2b.gtfs";

const USERNAME = process.env.NAP_USERNAME;
const PASSWORD = process.env.NAP_PASSWORD;
const OUT_FILE = process.env.GTFS_OUT || "gtfs.zip";

if (!USERNAME || !PASSWORD) {
  console.error("Missing env vars: NAP_USERNAME and/or NAP_PASSWORD");
  process.exit(1);
}

async function postToken(form) {
  const res = await fetch(TOKEN_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      Accept: "application/json",
    },
    body: new URLSearchParams(form).toString(),
  });

  const text = await res.text();
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    json = null;
  }

  if (!res.ok) {
    const msg = json?.error_description || json?.error || text || `HTTP ${res.status}`;
    throw new Error(`Token request failed (${res.status}): ${msg}`);
  }

  if (!json?.access_token) throw new Error("Token response missing access_token");
  return {
    access_token: json.access_token,
    refresh_token: json.refresh_token || null,
    expires_in: json.expires_in ?? null,
    token_type: json.token_type || "bearer",
  };
}

async function getTokenWithPassword() {
  return postToken({
    grant_type: "password",
    username: USERNAME,
    password: PASSWORD,
  });
}

async function refreshAccessToken(refreshToken) {
  return postToken({
    grant_type: "refresh_token",
    refresh_token: refreshToken,
  });
}

async function downloadGtfs(accessToken) {
  const res = await fetch(GTFS_URL, {
    headers: {
      // NAP docs typically show lowercase "bearer"
      Authorization: `bearer ${accessToken}`,
      Accept: "*/*",
    },
  });

  if (res.status === 401) {
    return { ok: false, status: 401 };
  }
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`GTFS download failed (${res.status}): ${body || "no body"}`);
  }

  await pipeline(Readable.fromWeb(res.body), fs.createWriteStream(OUT_FILE));
  return { ok: true, status: res.status };
}

async function main() {
  // 1) Get access + refresh via password grant
  let tok = await getTokenWithPassword();

  // 2) Try download with access token
  let dl = await downloadGtfs(tok.access_token);
  if (dl.ok) {
    console.log(`Downloaded: ${OUT_FILE}`);
    return;
  }

  // 3) If 401, try refresh once (if we have refresh_token)
  if (dl.status === 401 && tok.refresh_token) {
    console.log("Access token rejected (401). Refreshing token...");
    tok = await refreshAccessToken(tok.refresh_token);

    dl = await downloadGtfs(tok.access_token);
    if (dl.ok) {
      console.log(`Downloaded after refresh: ${OUT_FILE}`);
      return;
    }
  }

  // 4) If still 401, re-login once and retry
  if (dl.status === 401) {
    console.log("Still 401. Re-authenticating with password grant...");
    tok = await getTokenWithPassword();

    dl = await downloadGtfs(tok.access_token);
    if (dl.ok) {
      console.log(`Downloaded after re-auth: ${OUT_FILE}`);
      return;
    }
  }

  throw new Error(`Failed to download GTFS (last status: ${dl.status})`);
}

main().catch((e) => {
  console.error(e?.stack || String(e));
  process.exit(1);
});
