// scripts/nap-download-gtfs.js
//
// Downloads GTFS only if changed (ETag / Last-Modified).
// Stores cache metadata in .nap-cache.json (commit this file so Actions can compare).
// Shows detailed logs + download progress.
//
// Env:
//   NAP_USERNAME, NAP_PASSWORD (required)
//   GTFS_OUT (optional, default gtfs.zip)

const fs = require("node:fs");
const path = require("node:path");
const { Readable } = require("node:stream");
const { pipeline } = require("node:stream/promises");
const { Transform } = require("node:stream");

const TOKEN_URL = "https://b2b.nap.si/uc/user/token";
const GTFS_URL = "https://b2b.nap.si/data/b2b.gtfs";

const USERNAME = process.env.NAP_USERNAME;
const PASSWORD = process.env.NAP_PASSWORD;
const OUT_FILE = process.env.GTFS_OUT || "gtfs.zip";
const CACHE_FILE = ".nap-cache.json";

if (!USERNAME || !PASSWORD) {
  console.error("Missing env vars: NAP_USERNAME and/or NAP_PASSWORD");
  process.exit(1);
}

function nowIso() {
  return new Date().toISOString();
}

function log(...args) {
  console.log(`[${nowIso()}]`, ...args);
}

function readCache() {
  try {
    const p = path.resolve(CACHE_FILE);
    if (!fs.existsSync(p)) return {};
    return JSON.parse(fs.readFileSync(p, "utf8")) || {};
  } catch {
    return {};
  }
}

function writeCache(cache) {
  fs.writeFileSync(path.resolve(CACHE_FILE), JSON.stringify(cache, null, 2) + "\n", "utf8");
}

function pickHeaders(h) {
  const get = (k) => h.get(k);
  return {
    etag: get("etag") || null,
    lastModified: get("last-modified") || null,
    expires: get("expires") || null,
    age: get("age") || null,
    contentLength: get("content-length") || null,
  };
}

function buildConditionalHeaders(cache) {
  const h = {};
  if (cache?.etag) h["If-None-Match"] = cache.etag;
  if (cache?.lastModified) h["If-Modified-Since"] = cache.lastModified;
  return h;
}

function fmtBytes(n) {
  if (!Number.isFinite(n) || n <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  let x = n;
  while (x >= 1024 && i < units.length - 1) {
    x /= 1024;
    i++;
  }
  return `${x.toFixed(i === 0 ? 0 : 2)} ${units[i]}`;
}

function fmtPct(done, total) {
  if (!total) return "";
  return `${((done / total) * 100).toFixed(1)}%`;
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
  log("Requesting access token (password grant)...");
  const tok = await postToken({
    grant_type: "password",
    username: USERNAME,
    password: PASSWORD,
  });
  log("Got access token.", tok.expires_in != null ? `expires_in=${tok.expires_in}` : "");
  return tok;
}

async function refreshAccessToken(refreshToken) {
  log("Refreshing access token...");
  const tok = await postToken({
    grant_type: "refresh_token",
    refresh_token: refreshToken,
  });
  log("Refreshed access token.", tok.expires_in != null ? `expires_in=${tok.expires_in}` : "");
  return tok;
}

async function fetchGtfs(accessToken, method, extraHeaders) {
  return fetch(GTFS_URL, {
    method,
    headers: {
      Authorization: `bearer ${accessToken}`,
      Accept: "*/*",
      ...(extraHeaders || {}),
    },
  });
}

async function downloadToFileWithProgress(res, outFile) {
  const total = Number(res.headers.get("content-length") || 0);
  const startedAt = Date.now();
  let downloaded = 0;
  let lastBytes = 0;
  let lastT = startedAt;

  log(
    "Downloading GTFS...",
    total ? `size=${fmtBytes(total)}` : "(unknown size)",
  );

  const progress = new Transform({
    transform(chunk, _enc, cb) {
      downloaded += chunk.length;

      const t = Date.now();
      if (t - lastT >= 2000) {
        const dt = (t - lastT) / 1000;
        const dBytes = downloaded - lastBytes;
        const speed = dBytes / dt;

        const elapsed = (t - startedAt) / 1000;
        const pct = total ? ` ${fmtPct(downloaded, total)}` : "";
        const eta =
          total && speed > 0
            ? ` ETA ${Math.max(0, (total - downloaded) / speed).toFixed(0)}s`
            : "";

        log(
          `Progress: ${fmtBytes(downloaded)}${total ? ` / ${fmtBytes(total)}` : ""}${pct} | ` +
            `speed ${fmtBytes(speed)}/s | elapsed ${elapsed.toFixed(0)}s${eta}`,
        );

        lastT = t;
        lastBytes = downloaded;
      }

      cb(null, chunk);
    },
  });

  await pipeline(Readable.fromWeb(res.body), progress, fs.createWriteStream(outFile));

  const finishedAt = Date.now();
  const elapsed = (finishedAt - startedAt) / 1000;
  const avg = downloaded / Math.max(0.001, elapsed);
  log(`Download complete: ${outFile} (${fmtBytes(downloaded)} in ${elapsed.toFixed(1)}s, avg ${fmtBytes(avg)}/s)`);
}

async function downloadGtfs(accessToken, conditionalHeaders, cache) {
  log("Checking if GTFS changed (HEAD)...", conditionalHeaders && Object.keys(conditionalHeaders).length ? "(conditional)" : "");
  let head = await fetchGtfs(accessToken, "HEAD", conditionalHeaders);

  if (head.status === 401) return { ok: false, status: 401, notModified: false };
  if (head.status === 405 || head.status === 501) head = null;

  if (head) {
    const h = pickHeaders(head.headers);
    log(
      `HEAD status=${head.status}`,
      h.etag ? `ETag=${h.etag}` : "",
      h.lastModified ? `Last-Modified=${h.lastModified}` : "",
      h.expires ? `Expires=${h.expires}` : "",
      h.age ? `Age=${h.age}` : "",
      h.contentLength ? `Content-Length=${h.contentLength}` : "",
    );

    if (head.status === 304) {
      return { ok: true, status: 304, notModified: true, headers: h };
    }

    if (!head.ok) {
      const body = await head.text().catch(() => "");
      throw new Error(`GTFS HEAD failed (${head.status}): ${body || "no body"}`);
    }

    // If server doesn't send ETag/Last-Modified at all, we still continue with GET.
    // But if it does, and matches cache, some servers may still return 200 to HEAD; GET may return 304.
  } else {
    log("HEAD not supported. Will use GET directly.");
  }

  log("Fetching GTFS (GET)...", conditionalHeaders && Object.keys(conditionalHeaders).length ? "(conditional)" : "");
  const res = await fetchGtfs(accessToken, "GET", conditionalHeaders);

  if (res.status === 401) return { ok: false, status: 401, notModified: false };

  if (res.status === 304) {
    const h = pickHeaders(res.headers);
    log(
      `GET status=304 (not modified)`,
      h.etag ? `ETag=${h.etag}` : "",
      h.lastModified ? `Last-Modified=${h.lastModified}` : "",
    );
    return { ok: true, status: 304, notModified: true, headers: h };
  }

  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`GTFS download failed (${res.status}): ${body || "no body"}`);
  }

  const h = pickHeaders(res.headers);
  log(
    `GET status=${res.status}`,
    h.etag ? `ETag=${h.etag}` : "",
    h.lastModified ? `Last-Modified=${h.lastModified}` : "",
    h.expires ? `Expires=${h.expires}` : "",
    h.age ? `Age=${h.age}` : "",
    h.contentLength ? `Content-Length=${h.contentLength}` : "",
  );

  await downloadToFileWithProgress(res, OUT_FILE);
  return { ok: true, status: res.status, notModified: false, headers: h };
}

async function main() {
  const cache = readCache();
  const conditionalHeaders = buildConditionalHeaders(cache);

  if (cache?.etag || cache?.lastModified) {
    log(
      "Cache loaded:",
      cache.etag ? `ETag=${cache.etag}` : "",
      cache.lastModified ? `Last-Modified=${cache.lastModified}` : "",
    );
  } else {
    log("No cache present yet (first run).");
  }

  let tok = await getTokenWithPassword();

  let dl = await downloadGtfs(tok.access_token, conditionalHeaders, cache);
  if (dl.ok && dl.status !== 401) {
    if (dl.notModified) {
      log("GTFS not changed. Skipping download.");
      if (dl.headers) {
        writeCache({ ...cache, ...dl.headers, checkedAt: nowIso() });
        log(`Updated cache: ${CACHE_FILE}`);
      }
      return;
    }

    log(`Downloaded: ${OUT_FILE}`);
    if (dl.headers) {
      writeCache({ ...dl.headers, checkedAt: nowIso() });
      log(`Wrote cache: ${CACHE_FILE}`);
    }
    return;
  }

  if (dl.status === 401 && tok.refresh_token) {
    log("Access token rejected (401). Refreshing token and retrying...");
    tok = await refreshAccessToken(tok.refresh_token);

    dl = await downloadGtfs(tok.access_token, conditionalHeaders, cache);
    if (dl.ok && dl.status !== 401) {
      if (dl.notModified) {
        log("GTFS not changed. Skipping download.");
        if (dl.headers) {
          writeCache({ ...cache, ...dl.headers, checkedAt: nowIso() });
          log(`Updated cache: ${CACHE_FILE}`);
        }
        return;
      }

      log(`Downloaded after refresh: ${OUT_FILE}`);
      if (dl.headers) {
        writeCache({ ...dl.headers, checkedAt: nowIso() });
        log(`Wrote cache: ${CACHE_FILE}`);
      }
      return;
    }
  }

  if (dl.status === 401) {
    log("Still 401. Re-authenticating with password grant and retrying...");
    tok = await getTokenWithPassword();

    dl = await downloadGtfs(tok.access_token, conditionalHeaders, cache);
    if (dl.ok && dl.status !== 401) {
      if (dl.notModified) {
        log("GTFS not changed. Skipping download.");
        if (dl.headers) {
          writeCache({ ...cache, ...dl.headers, checkedAt: nowIso() });
          log(`Updated cache: ${CACHE_FILE}`);
        }
        return;
      }

      log(`Downloaded after re-auth: ${OUT_FILE}`);
      if (dl.headers) {
        writeCache({ ...dl.headers, checkedAt: nowIso() });
        log(`Wrote cache: ${CACHE_FILE}`);
      }
      return;
    }
  }

  throw new Error(`Failed to download GTFS (last status: ${dl.status})`);
}

main().catch((e) => {
  console.error(e?.stack || String(e));
  process.exit(1);
});
