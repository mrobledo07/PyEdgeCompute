import {
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import {
  createAwsS3Client,
  getAwsS3Client,
  obtainBucketAndObjectName,
  obtainBucketName,
} from "./awsS3Client.mjs";
//import { STORAGE_ORCH } from "./configAWS.mjs";
import { CONFIG } from "./main.mjs";

export async function getTextFromS3(fileUrl, offset = -1, numMappers = -1) {
  await createAwsS3Client();
  const { bucket, objectName } = obtainBucketAndObjectName(fileUrl);
  const client = getAwsS3Client();

  if (offset === -1) {
    // Get full object
    const command = new GetObjectCommand({ Bucket: bucket, Key: objectName });
    const response = await client.send(command);
    return streamToBuffer(response.Body);
  } else {
    // Get partial object (range)
    const headCmd = new HeadObjectCommand({ Bucket: bucket, Key: objectName });
    const metadata = await client.send(headCmd);
    const totalSize = metadata.ContentLength;
    const chunkSize = Math.floor(totalSize / numMappers);
    const start = offset * chunkSize;
    let end = (offset + 1) * chunkSize - 1;
    if (offset === numMappers - 1) end = totalSize - 1;

    const range = `bytes=${start}-${end}`;
    const command = new GetObjectCommand({
      Bucket: bucket,
      Key: objectName,
      Range: range,
    });
    const response = await client.send(command);
    return streamToBuffer(response.Body);
  }
}

async function streamToBuffer(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (chunk) => chunks.push(chunk));
    stream.on("end", () => resolve(Buffer.concat(chunks)));
    stream.on("error", reject);
  });
}

export async function getPartialObjectS3(task) {
  const offset = task.numWorker;
  const numMappers = task.numMappers;
  if (offset === undefined || numMappers === undefined) {
    throw new Error("Missing offset or numMappers");
  }
  const text = await getTextFromS3(task.arg, offset, numMappers);
  return text;
}

export async function getSerializedResults(results) {
  if (!Array.isArray(results) || results.length === 0)
    throw new Error("Invalid results array");
  const b64List = [];
  for (const result of results) {
    let partialResult = await getTextFromS3(result);
    b64List.push(partialResult.toString("utf-8"));
  }
  return b64List;
}

export async function setSerializedResult(task, result) {
  await createAwsS3Client();

  // Use STORAGE_ORCH as base URL, assuming like "https://bucket.s3.region.amazonaws.com"
  const bucket = obtainBucketName(CONFIG.STORAGE);
  const client = getAwsS3Client();

  // 1) Limpiar el taskId de su sufijo mapper/reducer
  let cleanedTaskId = task.taskId;
  let suffix = "";
  const m = task.taskId.match(/-(mapper\d+|reducer[\w\d]*)$/);
  if (m) {
    suffix = m[1];
    cleanedTaskId = task.taskId.replace(/-(mapper\d+|reducer[\w\d]*)$/, "");
  }

  const makeKey = (filename) => {
    return [
      task.clientId, // <— Aquí va siempre clientId
      cleanedTaskId, // luego el taskId "limpio"
      suffix, // opcional mapperX/reducerY
      filename, // "0-0.txt" o "0.txt"
    ]
      .filter(Boolean)
      .join("/");
  };

  if (Array.isArray(result)) {
    const urls = [];
    for (let i = 0; i < result.length; i++) {
      // Construimos el path con cleanedTaskId / suffix / task.numWorker-i.txt
      const filename = `${task.numWorker || 0}-${i}.txt`;
      const key = makeKey(filename);

      await client.send(
        new PutObjectCommand({
          Bucket: bucket,
          Key: key,
          Body: Buffer.from(result[i]),
          ContentType: "application/json",
        })
      );

      urls.push(`${CONFIG.STORAGE}/${key}`);
    }
    return urls;
  } else {
    const filename = `${task.numWorker || 0}.txt`;
    const key = makeKey(filename);

    await client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: Buffer.from(result),
        ContentType: "application/json",
      })
    );

    return `${CONFIG.STORAGE}/${key}`;
  }
}
