// s3Client.js
// English comments as requested.

import fs from "fs";
import path from "path";
import dns from "dns";

import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { fromInstanceMetadata } from "@aws-sdk/credential-provider-imds";
import { NodeHttpHandler } from "@aws-sdk/node-http-handler";
import { Agent as HttpsAgent } from "https";
import CacheableLookup from "cacheable-lookup";

/**
 * Simple async semaphore to limit concurrency.
 */
class Semaphore {
  constructor(max) {
    this.max = max;
    this.current = 0;
    this.queue = [];
  }

  acquire() {
    if (this.current < this.max) {
      this.current += 1;
      return Promise.resolve();
    }
    return new Promise((resolve) => {
      this.queue.push(resolve);
    }).then(() => {
      this.current += 1;
    });
  }

  release() {
    this.current = Math.max(0, this.current - 1);
    if (this.queue.length > 0) {
      const next = this.queue.shift();
      next();
    }
  }

  async use(fn) {
    await this.acquire();
    try {
      return await fn();
    } finally {
      this.release();
    }
  }
}

/* === Configuration defaults - tune these values === */
const DEFAULT_MAX_POOL_CONNECTIONS =
  Number(process.env.S3_MAX_POOL_CONNECTIONS) || 50; // agent pool
const DEFAULT_S3_CONCURRENCY = Number(process.env.S3_CONCURRENCY) || 6; // per-worker concurrent S3 ops
const DEFAULT_IMDS_TIMEOUT_MS = 1000;
const DEFAULT_IMDS_MAX_RETRIES = 3;
const DEFAULT_RETRY_ATTEMPTS = Number(process.env.S3_MAX_ATTEMPTS) || 10; // SDK-level retries

/* DNS error event buffer size (keep last N events) */
const DNS_ERROR_EVENTS_LIMIT =
  Number(process.env.DNS_ERROR_EVENTS_LIMIT) || 200;

/* Stacktrace log file path */
const STACKTRACE_FILE = process.env.STACKTRACE_FILE || "/var/log/stacktrace";

/* === Module state === */
let s3Client = null;
let credentialProvider = null;
const s3Semaphore = new Semaphore(DEFAULT_S3_CONCURRENCY);

/* DNS error stats (in-memory) */
const dnsErrorStats = {
  total: 0,
  byCode: {}, // e.g. { ENOTFOUND: 12, EAI_AGAIN: 3 }
  events: [], // { ts, code, hostname, attempt }
};

/* Create credential provider once (IMDS) */
export async function createCredentialProvider() {
  if (!credentialProvider) {
    credentialProvider = fromInstanceMetadata({
      timeout: DEFAULT_IMDS_TIMEOUT_MS,
      maxRetries: DEFAULT_IMDS_MAX_RETRIES,
    });
  }
}

/**
 * Append a message to the stacktrace log file. If writing fails (permissions),
 * fallback to console.error so we don't lose information.
 */
function appendStacktrace(message) {
  const timestamp = new Date().toISOString();
  const fullMsg = `[${timestamp}] ${message}\n`;
  try {
    fs.appendFileSync(STACKTRACE_FILE, fullMsg);
  } catch (err) {
    // If writing fails (likely permissions), log to console as fallback.
    console.error("[STACKTRACE WRITE FAIL]", err);
    console.error(fullMsg);
  }
}

/**
 * Small helper: sleep ms
 */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Record a DNS error event into memory and optionally print a concise log line.
 * Also persist an entry in the stacktrace file.
 */
function recordDnsErrorEvent({ code, hostname, attempt }) {
  dnsErrorStats.total += 1;
  dnsErrorStats.byCode[code] = (dnsErrorStats.byCode[code] || 0) + 1;
  const ev = {
    ts: Date.now(),
    code,
    hostname: hostname || "unknown",
    attempt: attempt || 0,
  };
  dnsErrorStats.events.push(ev);
  // Trim buffer
  if (dnsErrorStats.events.length > DNS_ERROR_EVENTS_LIMIT) {
    dnsErrorStats.events.shift();
  }
  // Log concise message for monitoring
  const concise = `[DNS ERROR RECORDED] code=${code} hostname=${ev.hostname} attempt=${ev.attempt} total=${dnsErrorStats.total}`;
  console.warn(concise);
  appendStacktrace(concise);
}

/**
 * Retry wrapper specifically to handle DNS lookup transient errors.
 * It retries on common DNS-related error codes ('ENOTFOUND', 'EAI_AGAIN').
 * Uses exponential backoff with small jitter.
 *
 * Also records DNS errors into dnsErrorStats and the stacktrace file.
 */
async function withDnsRetry(fn, maxAttempts = 3) {
  let attempt = 0;
  while (true) {
    try {
      return await fn();
    } catch (err) {
      attempt += 1;
      const code = err && (err.code || err.name);
      const isDnsError =
        code === "ENOTFOUND" || code === "EAI_AGAIN" || code === "ENODATA";
      // Attempt to extract hostname from common error fields
      const hostname =
        err &&
        (err.hostname || err.host || err.address || err.hostname || null);

      if (isDnsError) {
        // record the DNS event immediately (this also writes to stacktrace)
        recordDnsErrorEvent({ code, hostname, attempt });
      }

      if (isDnsError && attempt < maxAttempts) {
        // exponential backoff with jitter: base 50ms
        const base = 50 * Math.pow(2, attempt - 1);
        const jitter = Math.floor(Math.random() * 50);
        const wait = base + jitter;
        const warnMsg = `[DNS RETRY] DNS error (${code}), attempt ${attempt}/${maxAttempts}, retrying after ${wait}ms, hostname=${hostname}`;
        console.warn(warnMsg);
        appendStacktrace(warnMsg);
        await sleep(wait);
        continue;
      }
      // Not a DNS transient error or max attempts exceeded -> log and rethrow
      const errMsg = `[DNS FINAL] code=${code} hostname=${hostname} attempt=${attempt} message=${
        err?.message || String(err)
      }`;
      appendStacktrace(errMsg);
      throw err;
    }
  }
}

/**
 * Create a singleton S3 client for this worker process.
 * This should be called once (or via createAwsS3ClientOnce()) before using the client.
 */
export async function createAwsS3ClientOnce() {
  if (s3Client) return; // already created

  await createCredentialProvider();

  // Create an HTTPS Agent with keepAlive and a larger connection pool
  const httpsAgent = new HttpsAgent({
    keepAlive: true,
    maxSockets: DEFAULT_MAX_POOL_CONNECTIONS,
    maxFreeSockets: Math.min(DEFAULT_MAX_POOL_CONNECTIONS, 10),
  });

  // Install DNS cache on the agent to avoid repeated DNS lookups under bursty load
  const cacheable = new CacheableLookup();
  try {
    cacheable.install(httpsAgent);
  } catch (err) {
    // If for some reason installing cache fails, record it but continue.
    appendStacktrace(`[CACHEABLE-LOOKUP FAIL] ${err?.message || String(err)}`);
  }

  // Node HTTP handler using the agent
  const httpHandler = new NodeHttpHandler({
    httpsAgent,
    // connectTimeout could be tuned; default is reasonable
  });

  s3Client = new S3Client({
    region: "eu-north-1",
    credentials: credentialProvider,
    requestHandler: httpHandler,
    // control retry behaviour
    maxAttempts: DEFAULT_RETRY_ATTEMPTS,
    retryMode: "adaptive",
    // avoid virtual-hosted style creating bucket-based hostnames
    s3ForcePathStyle: true,
    disableHostPrefix: true,
    // optional: logger: console, // enable to let SDK print warnings
  });

  /**
   * Pre-request DNS probe middleware:
   * - logs the hostname being requested
   * - runs dns.lookup(hostname) to detect resolution failures immediately
   * - writes probe results to stacktrace for offline analysis
   */
  s3Client.middlewareStack.add(
    (next, context) => async (args) => {
      try {
        const req = args?.request || {};
        // try to extract hostname sensibly
        const hostname =
          req.hostname ||
          req.host ||
          // args.request.path may be like 's3.amazonaws.com/bucket/object' or '/bucket/object'
          (typeof req.path === "string"
            ? req.path.split("/")[0] || null
            : null) ||
          null;

        const probeMsg = `[S3 REQUEST] op=${context.commandName} hostname=${hostname}`;
        console.log(probeMsg);
        appendStacktrace(probeMsg);

        if (hostname) {
          const start = Date.now();
          try {
            // use system resolver via dns.promises.lookup
            await dns.promises.lookup(hostname);
            const ms = Date.now() - start;
            const okMsg = `[DNS OK] ${hostname} resolved in ${ms}ms`;
            console.log(okMsg);
            appendStacktrace(okMsg);
          } catch (dnsErr) {
            const ms = Date.now() - start;
            const failMsg = `[DNS FAIL] ${hostname} - code=${
              dnsErr.code || dnsErr.name
            } after ${ms}ms`;
            console.warn(failMsg);
            appendStacktrace(failMsg);
            // continue to next middleware/request so normal retry logic can run
          }
        }

        return await next(args);
      } catch (err) {
        // If the pre-request probe itself throws, log and continue with the request (do not block)
        const probeErr = `[PRE-REQUEST PROBE ERROR] op=${
          context.commandName
        } err=${err?.message || String(err)}`;
        appendStacktrace(probeErr);
        // rethrow or continue? Continue to allow request to proceed to SDK retries.
        return await next(args);
      }
    },
    { step: "build", name: "preRequestDnsProbe", priority: "high" }
  );

  // Lightweight middleware to log status codes and errors (helps debugging 4xx/5xx)
  s3Client.middlewareStack.add(
    (next, context) => async (args) => {
      try {
        const result = await next(args);
        // log response status if available (useful for diagnosing 4xx/5xx)
        const status = result?.response?.statusCode;
        if (status && status >= 400) {
          const warnMsg = `[S3 HTTP WARNING] op=${context.commandName} status=${status} request=${args?.request?.method} ${args?.request?.path}`;
          console.warn(warnMsg);
          appendStacktrace(warnMsg);
        }
        return result;
      } catch (err) {
        // Log the error and rethrow - includes network/timeout/retry errors
        const errMsg = `[S3 ERROR] op=${context.commandName} code=${
          err?.code || err?.name
        } message=${err?.message || String(err)} stack=${
          err?.stack || "no-stack"
        }`;
        console.error(errMsg);
        appendStacktrace(errMsg);
        throw err;
      }
    },
    {
      step: "finalizeRequest",
      name: "s3StatusLoggerMiddleware",
      priority: "low",
    }
  );

  // Force credentials resolution early so we fail fast if IMDS is unavailable
  try {
    await s3Client.config.credentials();
    const okCredMsg = "[INFO awsS3Client] AWS credentials loaded successfully";
    console.log(okCredMsg);
    appendStacktrace(okCredMsg);
  } catch (err) {
    const credErr = `[ERROR awsS3Client] Failed to load AWS credentials: ${
      err?.message || String(err)
    }`;
    console.error(credErr);
    appendStacktrace(credErr);
    throw err;
  }
}

/**
 * Get the singleton S3 client. Will throw if not created.
 */
export function getAwsS3Client() {
  if (!s3Client) {
    throw new Error(
      "AWS S3 client not initialized. Call createAwsS3ClientOnce() first."
    );
  }
  return s3Client;
}

/* === Convenience wrappers that use the semaphore and DNS-retry wrapper ===
   These wrapper functions ensure we never exceed the per-worker concurrent S3 ops limit,
   and also retry DNS lookup transient errors.
   Use these helpers from your mapper/reducer code instead of calling client.send(...) directly.
*/

export async function s3GetObject(bucket, key) {
  return s3Semaphore.use(async () => {
    const client = getAwsS3Client();
    const cmd = new GetObjectCommand({ Bucket: bucket, Key: key });
    return withDnsRetry(() => client.send(cmd));
  });
}

export async function s3PutObject(bucket, key, body) {
  return s3Semaphore.use(async () => {
    const client = getAwsS3Client();
    const cmd = new PutObjectCommand({ Bucket: bucket, Key: key, Body: body });
    return withDnsRetry(() => client.send(cmd));
  });
}

/* === DNS error stats helpers ===
   Expose functions to read/reset the in-memory DNS error statistics.
   These can be polled from your orchestration/logging to understand how often ENOTFOUND occurs.
*/
export function getDnsErrorStats() {
  // Return a shallow copy to avoid external mutation
  return {
    total: dnsErrorStats.total,
    byCode: { ...dnsErrorStats.byCode },
    events: dnsErrorStats.events.slice(), // copy of array
  };
}

export function resetDnsErrorStats() {
  dnsErrorStats.total = 0;
  dnsErrorStats.byCode = {};
  dnsErrorStats.events = [];
}

/* Keep your existing helpers for parsing s3 urls */
export function obtainBucketName(fileUrl) {
  if (!fileUrl.startsWith("s3://")) {
    throw new Error("Invalid S3 URL format: must start with 's3://'");
  }
  const withoutPrefix = fileUrl.slice("s3://".length);
  if (withoutPrefix.includes("/")) {
    throw new Error("Expected only bucket name, but got a full S3 path");
  }
  const bucket = withoutPrefix.trim();
  if (!bucket) {
    throw new Error("Bucket name is empty");
  }
  console.log("OBTAINING BUCKET ORCHESTRATOR");
  console.log("BUCKET: ", bucket);
  return bucket;
}

const S3_URL_REGEX = /^s3:\/\/([^\/]+)\/(.+)$/;
export function obtainBucketAndObjectName(fileUrl) {
  const trimmed = String(fileUrl).trim();
  const match = S3_URL_REGEX.exec(trimmed);
  if (!match) {
    throw new Error("Invalid S3 URL, must match s3://bucket/object");
  }
  const [, bucket, objectName] = match;
  return { bucket, objectName };
}
