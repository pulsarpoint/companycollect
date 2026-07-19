import "dotenv/config";
import { createHash, createHmac } from "node:crypto";

/**
 * Minimal SigV4 GET against the corpscout object store (RustFS, S3 API).
 * Path-style addressing; no SDK dependency — we only ever need GetObject.
 */

const REGION = "us-east-1";
const SERVICE = "s3";
const EMPTY_SHA256 = createHash("sha256").update("").digest("hex");

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

export async function fetchObject(bucket: string, key: string): Promise<Response> {
  const endpoint = process.env.CORPSCOUT_S3_ENDPOINT;
  const accessKey = process.env.CORPSCOUT_S3_ACCESS_KEY;
  const secretKey = process.env.CORPSCOUT_S3_SECRET_KEY;
  if (!endpoint || !accessKey || !secretKey) {
    throw new Error("CORPSCOUT_S3_ENDPOINT/ACCESS_KEY/SECRET_KEY not configured");
  }

  const url = new URL(endpoint);
  const host = url.host;
  const canonicalUri = `/${bucket}/${encodeObjectKey(key)}`;

  const now = new Date();
  const amzDate = now.toISOString().replace(/[-:]/g, "").replace(/\.\d{3}/, "");
  const dateStamp = amzDate.slice(0, 8);

  const canonicalHeaders = `host:${host}\nx-amz-content-sha256:${EMPTY_SHA256}\nx-amz-date:${amzDate}\n`;
  const signedHeaders = "host;x-amz-content-sha256;x-amz-date";
  const canonicalRequest = `GET\n${canonicalUri}\n\n${canonicalHeaders}\n${signedHeaders}\n${EMPTY_SHA256}`;

  const credentialScope = `${dateStamp}/${REGION}/${SERVICE}/aws4_request`;
  const stringToSign = `AWS4-HMAC-SHA256\n${amzDate}\n${credentialScope}\n${createHash("sha256")
    .update(canonicalRequest)
    .digest("hex")}`;

  const kDate = hmac(`AWS4${secretKey}`, dateStamp);
  const kRegion = hmac(kDate, REGION);
  const kService = hmac(kRegion, SERVICE);
  const kSigning = hmac(kService, "aws4_request");
  const signature = hmac(kSigning, stringToSign).toString("hex");

  const authorization = `AWS4-HMAC-SHA256 Credential=${accessKey}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;

  return fetch(`${url.origin}${canonicalUri}`, {
    headers: {
      Authorization: authorization,
      "x-amz-content-sha256": EMPTY_SHA256,
      "x-amz-date": amzDate,
    },
  });
}
