import "dotenv/config";
import { createHash, createHmac } from "node:crypto";

/**
 * Minimal SigV4 requests against the corpscout object store (RustFS, S3 API).
 * Path-style addressing; no SDK dependency -- we need GetObject, PutObject and
 * creating a bucket, nothing else.
 */

const REGION = "us-east-1";
const SERVICE = "s3";
const EMPTY_SHA256 = sha256Hex("");

export interface ObjectStoreOptions {
  fetchImpl?: typeof fetch;
  /** Fixed clock for tests. */
  now?: Date;
}

export class ObjectStoreError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ObjectStoreError";
  }
}

function sha256Hex(data: string | Uint8Array): string {
  return createHash("sha256").update(data).digest("hex");
}

function hmac(key: Buffer | string, data: string): Buffer {
  return createHmac("sha256", key).update(data).digest();
}

/** RFC 3986 encode each path segment, keeping the slashes. */
function encodeObjectKey(key: string): string {
  return key
    .split("/")
    .map((seg) => encodeURIComponent(seg).replace(/[!'()*]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`))
    .join("/");
}

function credentials() {
  const endpoint = process.env.CORPSCOUT_S3_ENDPOINT;
  const accessKey = process.env.CORPSCOUT_S3_ACCESS_KEY;
  const secretKey = process.env.CORPSCOUT_S3_SECRET_KEY;
  if (!endpoint || !accessKey || !secretKey) {
    throw new Error("CORPSCOUT_S3_ENDPOINT/ACCESS_KEY/SECRET_KEY not configured");
  }
  return { endpoint, accessKey, secretKey };
}

/** One signed request. `path` is `/<bucket>` or `/<bucket>/<encoded key>`. */
function signedRequest(
  method: "GET" | "PUT" | "HEAD",
  path: string,
  body: Uint8Array<ArrayBuffer> | null,
  options: ObjectStoreOptions = {},
): Promise<Response> {
  const { endpoint, accessKey, secretKey } = credentials();
  const url = new URL(endpoint);
  const host = url.host;
  const payloadHash = body ? sha256Hex(body) : EMPTY_SHA256;

  const amzDate = (options.now ?? new Date()).toISOString().replace(/[-:]/g, "").replace(/\.\d{3}/, "");
  const dateStamp = amzDate.slice(0, 8);

  const canonicalHeaders = `host:${host}\nx-amz-content-sha256:${payloadHash}\nx-amz-date:${amzDate}\n`;
  const signedHeaders = "host;x-amz-content-sha256;x-amz-date";
  const canonicalRequest = `${method}\n${path}\n\n${canonicalHeaders}\n${signedHeaders}\n${payloadHash}`;

  const credentialScope = `${dateStamp}/${REGION}/${SERVICE}/aws4_request`;
  const stringToSign = `AWS4-HMAC-SHA256\n${amzDate}\n${credentialScope}\n${sha256Hex(canonicalRequest)}`;

  const kDate = hmac(`AWS4${secretKey}`, dateStamp);
  const kRegion = hmac(kDate, REGION);
  const kService = hmac(kRegion, SERVICE);
  const kSigning = hmac(kService, "aws4_request");
  const signature = hmac(kSigning, stringToSign).toString("hex");

  const authorization = `AWS4-HMAC-SHA256 Credential=${accessKey}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;

  return (options.fetchImpl ?? fetch)(`${url.origin}${path}`, {
    method,
    headers: {
      Authorization: authorization,
      "x-amz-content-sha256": payloadHash,
      "x-amz-date": amzDate,
    },
    ...(body ? { body } : {}),
  });
}

export async function fetchObject(bucket: string, key: string, options: ObjectStoreOptions = {}): Promise<Response> {
  return signedRequest("GET", `/${bucket}/${encodeObjectKey(key)}`, null, options);
}

/** Create `bucket` when a HEAD says it is not there. Idempotent. */
export async function ensureBucket(bucket: string, options: ObjectStoreOptions = {}): Promise<void> {
  const head = await signedRequest("HEAD", `/${bucket}`, null, options);
  if (head.ok) return;
  if (head.status !== 404) {
    throw new ObjectStoreError(`Checking bucket ${bucket} failed: HTTP ${head.status}.`);
  }
  const created = await signedRequest("PUT", `/${bucket}`, null, options);
  // 409 BucketAlreadyOwnedByYou: someone created it between the HEAD and the PUT.
  if (!created.ok && created.status !== 409) {
    throw new ObjectStoreError(`Creating bucket ${bucket} failed: HTTP ${created.status} ${await created.text()}`);
  }
}

/** PutObject with the payload's sha256 signed (not UNSIGNED-PAYLOAD). */
export async function putObject(
  bucket: string,
  key: string,
  body: Uint8Array<ArrayBuffer>,
  options: ObjectStoreOptions = {},
): Promise<void> {
  const response = await signedRequest("PUT", `/${bucket}/${encodeObjectKey(key)}`, body, options);
  if (!response.ok) {
    throw new ObjectStoreError(`Uploading ${key} to ${bucket} failed: HTTP ${response.status} ${await response.text()}`);
  }
}
