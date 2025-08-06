import { S3Client } from "@aws-sdk/client-s3";
import { fromInstanceMetadata } from "@aws-sdk/credential-provider-imds";

//import "dotenv/config";

let s3Client;

// export function createAwsS3Client() {
//   s3Client = new S3Client({
//     region: "eu-north-1",
//     // credentials: {
//     //   accessKeyId: process.env.AWS_ACCESS_KEY_ID,
//     //   secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
//     //   sessionToken: process.env.AWS_SESSION_TOKEN,
//     // },
//   });
// }

export async function createAwsS3Client() {
  // Usamos fromInstanceMetadata con reintentos internos
  const credentialProvider = fromInstanceMetadata({
    timeout: 1000, // milisegundos antes de timeout en IMDS
    maxRetries: 3, // reintentos de credenciales
  });

  s3Client = new S3Client({
    region: "eu-north-1",
    credentials: credentialProvider,
  });

  // Forzamos la obtención de credenciales *antes* de usar el client
  try {
    await s3Client.config.credentials();
    console.log("[INFO awsS3Client] AWS credentials loaded successfully");
  } catch (err) {
    console.error("[ERROR awsS3Client] Failed to load AWS credentials:", err);
    throw err;
  }
}

export function getAwsS3Client() {
  if (!s3Client) throw new Error("AWS S3 client not initialized");
  return s3Client;
}

export function obtainBucketName(fileUrl) {
  if (!fileUrl.startsWith("s3://")) {
    throw new Error("Invalid S3 URL format: must start with 's3://'");
  }

  const withoutPrefix = fileUrl.slice("s3://".length);

  // Asegúrate de que no contenga una barra, porque eso implicaría que hay una ruta después del bucket
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
