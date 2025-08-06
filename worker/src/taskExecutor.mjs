import {
  getTextFromMinio,
  getPartialObjectMinio,
  getSerializedResults as getSerializedResultsMinio,
  setSerializedResult as setSerializedResultMinio,
} from "./minioStorage.mjs";

import {
  getTextFromS3,
  getPartialObjectS3,
  getSerializedResults as getSerializedResultsS3,
  setSerializedResult as setSerializedResultS3,
} from "./awsS3Storage.mjs"; // <-- asegúrate de tener estas funciones definidas como en la respuesta anterior

import { getPyodide } from "./pyodideRuntime.mjs";

export async function executeTask(task, ws, stopWatch) {
  const pyodide = getPyodide();
  stopWatch.start();
  const initTime = Date.now() / 1000;
  let readTime = null;
  let cpuTime = null;
  let writeTime = null;
  let endTime = null;

  try {
    console.log("RECEIVING CLIENT ID:", task.clientId);
    let bytes;

    const isS3 = (url) => {
      if (Array.isArray(url)) {
        return url.every((u) => typeof u === "string" && u.startsWith("s3://"));
      }
      return typeof url === "string" && url.startsWith("s3://");
    };

    if (task.type === "mapwordcount" || task.type === "mapterasort") {
      console.log(
        `🔍 Getting partial object for MAPPER task ${task.arg}:${task.taskId}`
      );
      bytes = isS3(task.arg)
        ? await getPartialObjectS3(task)
        : await getPartialObjectMinio(task);
    } else if (
      task.type === "reducewordcount" ||
      task.type === "reduceterasort"
    ) {
      console.log(
        `🔍 Getting serialized results for REDUCER task ${task.arg}:${task.taskId}`
      );

      bytes = isS3(task.arg)
        ? await getSerializedResultsS3(task.arg)
        : await getSerializedResultsMinio(task.arg);
    } else {
      console.log(
        `🔍 Getting full object for task ${task.arg}:${task.taskId} (not map or reduce)`
      );
      bytes = isS3(task.arg)
        ? await getTextFromS3(task.arg)
        : await getTextFromMinio(task.arg);
    }

    let rawBytesLine;
    if (task.type === "reduceterasort" || task.type === "reducewordcount") {
      rawBytesLine = `raw_bytes = ${JSON.stringify(bytes)}`;
      // console.log(
      //   `🔍 raw_bytes for REDUCER task ${task.taskId} is: ${rawBytesLine}`
      // );
    } else {
      const b64 = bytes.toString("base64");
      rawBytesLine = `raw_bytes = base64.b64decode("${b64}")`;
    }

    const roundTo4 = (num) => Math.round(num * 10000) / 10000;

    stopWatch.stop();
    readTime = roundTo4(stopWatch.getDuration());

    console.log("INPUT SIZE", bytes.length);

    let extraPythonVars = "";
    if (task.type === "mapterasort") {
      console.log("MAPTERASORT");
      console.log("NUM_PARTITIONS: ", task.numReducers);
      extraPythonVars = `num_partitions = ${task.numReducers}`;
    }

    const pyScript = `
${task.code}
${rawBytesLine}
${extraPythonVars}
try:
    result = task(raw_bytes)
except Exception as e:
    result = str(e)
result
    `;

    console.log(
      `📜 Executing task ${task.taskId} from client ${task.clientId} with arg ${task.arg}`
    );

    // console.log("CODE:");
    // console.log(pyScript);

    stopWatch.start();
    let result = await pyodide.runPythonAsync(pyScript);
    stopWatch.stop();
    cpuTime = roundTo4(stopWatch.getDuration());

    console.log(
      `✔️ Completed task ${task.taskId} from client ${task.clientId} with arg ${task.arg}`
    );

    if (task.type === "mapterasort") {
      result = JSON.parse(result);
    }

    stopWatch.start();

    const resultURL = isS3(task.arg)
      ? await setSerializedResultS3(task, result)
      : await setSerializedResultMinio(task, result);

    stopWatch.stop();
    writeTime = roundTo4(stopWatch.getDuration());
    endTime = Date.now() / 1000;

    ws.send(
      JSON.stringify({
        clientId: task.clientId,
        taskId: task.taskId,
        status: "done",
        result: resultURL,
        initTime,
        readTime,
        cpuTime,
        writeTime,
        endTime,
      })
    );
  } catch (e) {
    console.error(
      `❌ Error on task ${task.taskId} from client ${task.clientId} with arg ${task.arg}`,
      e.message
    );
    console.error(e);
    ws.send(
      JSON.stringify({
        clientId: task.clientId,
        taskId: task.taskId,
        status: "error",
        result: e.message,
        initTime,
        readTime,
        cpuTime,
        writeTime,
        endTime,
      })
    );
    stopWatch.stop();
  }
}
