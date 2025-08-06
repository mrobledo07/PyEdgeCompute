import {
  getMinioClient,
  createMinioClient,
  obtainBucketAndObjectName,
} from "./minioClient.mjs";

// import { CONFIG.STORAGE } from "./configMinio.mjs";
import { CONFIG } from "./main.mjs";

export async function getTextFromMinio(fileUrl, offset = -1, numMappers = -1) {
  createMinioClient(fileUrl);
  const minioClient = getMinioClient();
  const { bucket, objectName } = obtainBucketAndObjectName(fileUrl);
  let stream;
  if (offset == -1) {
    stream = await minioClient.getObject(bucket, objectName);
  } else {
    // If offset is provided, get a partial object
    const stat = await minioClient.statObject(bucket, objectName);
    const totalSize = stat.size;
    const chunkSize = Math.floor(totalSize / numMappers);
    const start = offset * chunkSize;
    let end = (offset + 1) * chunkSize - 1;

    // Assign the last chunk to the last mapper
    if (offset === numMappers - 1) end = totalSize - 1;

    stream = await minioClient.getPartialObject(bucket, objectName, start, end);
  }
  return new Promise((res, rej) => {
    const chunks = [];
    stream.on("data", (c) => chunks.push(c));
    stream.on("end", () => res(Buffer.concat(chunks)));
    stream.on("error", (e) => rej(e));
  });
}

export async function getPartialObjectMinio(task) {
  const offset = task.numWorker;
  const numMappers = task.numMappers;
  console.log(
    `🔍 Getting partial object from Minio: offset=${offset}, numMappers=${numMappers}`
  );
  if (offset === undefined || numMappers === undefined) {
    throw new Error(
      "Invalid task received. Missing offset or num of mappers for MAPPER partial object."
    );
  }
  console.log(`🔍 Getting TASK ARG ${task.arg}`);
  const text = await getTextFromMinio(task.arg, offset, numMappers);
  return text;
}

export async function getSerializedResults(results) {
  if (!Array.isArray(results) || results.length === 0)
    throw new Error(
      "Invalid task received. Missing array of results for REDUCER."
    );

  const b64List = [];
  for (const result of results) {
    let partialResult = await getTextFromMinio(result);
    //console.log("🔍 First partial:", partialResult);
    b64List.push(partialResult.toString("utf-8"));
  }
  //console.log("🔍 Mappers results aggregated:", resultJSON);
  //console.log("🔍 Returning serialized results for REDUCER:", b64List);
  // Return the
  return b64List;
}

export async function setSerializedResult(task, result) {
  // Construimos la basePath con clientId y taskId original (se usará en URL de retorno)
  const basePath = `${CONFIG.STORAGE}/${task.clientId}`;

  createMinioClient(basePath);
  const minioClient = getMinioClient();
  const { bucket } = obtainBucketAndObjectName(basePath);

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
      cleanedTaskId, // luego el taskId "limpio"
      suffix, // opcional mapperX/reducerY
      filename, // "0-0.txt" o "0.txt"
    ]
      .filter(Boolean)
      .join("/");
  };

  try {
    await minioClient.makeBucket(bucket, "us-east-1");
  } catch (e) {
    if (e.code !== "BucketAlreadyOwnedByYou") {
      console.error(`❌ Error creating bucket ${bucket}:`, e.message);
      throw e;
    }
    console.log(`ℹ️ Bucket ${bucket} already exists, skipping creation.`);
  }

  if (Array.isArray(result)) {
    const urls = [];
    for (let i = 0; i < result.length; i++) {
      const reducerResult = result[i];
      // Construimos el objectName con cleanedTaskId, suffix y numWorker-i
      const filename = `${task.numWorker || 0}-${i}.txt`;
      const key = makeKey(filename);
      console.log(`📦 Storing reducer result in Minio: ${basePath}/${key}`);
      try {
        await minioClient.putObject(
          bucket,
          key,
          Buffer.from(reducerResult),
          reducerResult.length,
          "application/json"
        );
        urls.push(`${basePath}/${key}`);
      } catch (e) {
        console.error(`❌ Error storing reducer result [${i}]:`, e.message);
        throw e;
      }
    }
    return urls;
  } else {
    const filename = `${task.numWorker || 0}.txt`;
    const key = makeKey(filename);

    console.log(`📦 Storing result in Minio: ${basePath}/${key}`);
    try {
      await minioClient.putObject(
        bucket,
        key,
        Buffer.from(result),
        result.length,
        "application/json"
      );
      return `${basePath}/${key}`;
    } catch (e) {
      console.error(`❌ Error storing result in Minio:`, e.message);
      throw e;
    }
  }
}
