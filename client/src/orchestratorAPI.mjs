// orchestratorAPI.js
import axios from "axios";
import WebSocket from "ws";
import readline from "readline/promises";
import fs from "fs";
import path from "path";

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

// export async function connectClient(clientId, httpUrl) {
//   try {
//     const res = await axios.post(`${httpUrl}/client_connect`, {
//       client_id: clientId,
//     });
//     console.log("🔌 Connected to orchestrator. CLIENT ID:", res.data.client_id);
//     return {
//       results: res.data.results,
//       pendingTasks: res.data.pendingTasks || 0,
//     };
//   } catch (err) {
//     if (err.response) {
//       console.error("❌ Response error:", err.response.data || err.message);
//     } else if (err.request) {
//       console.error("❌ No response from orchestrator.");
//     } else {
//       console.error("❌ Error:", err.message);
//     }
//   }
// }

export async function sendTaskWithRetry(task, httpUrl) {
  while (true) {
    try {
      const res = await axios.post(`${httpUrl}/register_task`, task);
      console.log("🚀 Tasks submitted. CLIENT ID:", res.data.client_id);
      return res.data.client_id;
    } catch (err) {
      if (err.response) {
        console.error("❌ Response error:", err.response.data || err.message);
      } else if (err.request) {
        console.error("❌ No response from orchestrator.");
      } else {
        console.error("❌ Error:", err.message);
      }

      try {
        await rl.question("\n🔁 Press Enter to retry, or Ctrl+C to exit...\n");
      } catch {
        console.log("\n👋 Exiting...");
        process.exit(1);
      }
    }
  }
}

function printTaskObject(task) {
  console.log("\n🧾 Full Task Object:\n");
  console.log(JSON.stringify(task, null, 2)); // Pretty-print full JSON

  if (task.metadata && Object.keys(task.metadata).length > 0) {
    console.log("\n📊 Parsed Execution Metadata:");
    printMetadata(task.metadata); // Usa la función anterior
  } else {
    console.log("\nℹ️ No metadata available.");
  }
}

function printMetadata(metadata) {
  Object.entries(metadata).forEach(([taskId, times], index) => {
    if (!Array.isArray(times) || times.length !== 5) {
      console.warn(`⚠️ Invalid metadata format for ${taskId}`);
      return;
    }

    const [startTime, readTime, cpuTime, writeTime, endTime] = times;
    const totalTime = (endTime - startTime).toFixed(4);

    console.log(`🔹 Task ${index + 1}: ${taskId}`);
    console.log(`   • Start Time : ${startTime.toFixed(3)}s`);
    console.log(`   • Read Time  : ${readTime.toFixed(4)}s`);
    console.log(`   • CPU Time   : ${cpuTime.toFixed(4)}s`);
    console.log(`   • Write Time : ${writeTime.toFixed(4)}s`);
    console.log(`   • End Time   : ${endTime.toFixed(3)}s`);
    console.log(`   • Total Time : ${totalTime}s\n`);
  });
}

/**
 * @param {string} wsUrl
 * @param {string} clientId
 * @param {number} maxTasks
 * @param {Stopwatch[]} stopwatches
 * @param {number} sentTime
 * @param {string|null} outDir
 */
export function connectToWebSocket(
  wsUrl,
  clientId,
  maxTasks,
  stopwatches,
  sentTime,
  outDir = null
) {
  const ws = new WebSocket(`${wsUrl}?client_id=${clientId}`);
  let tasksExecuted = 0;

  ws.on("open", () => {
    console.log(
      "🔌 Connected to orchestrator via WebSocket. CLIENT ID:",
      clientId
    );
  });

  ws.on("message", async (data) => {
    try {
      const parsedData = JSON.parse(data.toString());
      if (!parsedData) {
        console.warn("⚠️ Received malformed message:", data.toString());
        return;
      }
      let task;

      if (parsedData.type.startsWith("mapreduce")) {
        task = {
          message_type: "task_result",
          type: parsedData.type,
          numMappers: parsedData.numMappers,
          numReducers: parsedData.numReducers,
          taskId: parsedData.taskId,
          status: parsedData.status,
          result: parsedData.result,
          metadata: parsedData.metadata || {},
        };
      } else {
        task = {
          message_type: "task_result",
          type: parsedData.type,
          taskId: parsedData.taskId,
          status: parsedData.status,
          result: parsedData.result,
          metadata: parsedData.metadata || {},
        };
      }
      // const { message_type, taskId, status, result, metadata } = JSON.parse(
      //   data.toString()
      // );

      //const metadataParsed = metadata ? JSON.parse(metadata) : {};

      switch (task.message_type) {
        case "task_result":
          stopwatches[tasksExecuted]?.stop();
          const executionTimeClient = parseFloat(
            stopwatches[tasksExecuted]?.getDuration().toFixed(4)
          );
          tasksExecuted++;
          //const roundTo4 = (num) => Math.round(num * 10000) / 10000;

          const receivedTime = Date.now() / 1000; // Convert to seconds
          console.log(`📦 Task ${task.taskId} completed.`);
          console.log(
            `⏱️ Execution time: ${executionTimeClient}s (sent at ${sentTime}, received at ${receivedTime})`
          );

          let base = task.type;
          if (typeof task.numMappers === "number") {
            base += `_m${task.numMappers}`;
          }
          if (typeof task.numReducers === "number") {
            base += `_r${task.numReducers}`;
          }
          const filename = `${base}.json`;
          if (outDir) {
            try {
              await fs.promises.mkdir(outDir, { recursive: true });
              const filePath = path.join(outDir, filename);
              await fs.promises.writeFile(
                filePath,
                JSON.stringify(task, null, 2),
                "utf-8"
              );
              console.log(`💾 Saved task output to ${filePath}`);
            } catch (err) {
              console.error("❌ Failed to write output file:", err.message);
            }
          }

          printTaskObject(task);
          // console.log(`Status: ${task.status}`);
          // console.log(`Result: ${task.result}`);

          // printMetadata(task.metadata);
          if (tasksExecuted >= maxTasks) {
            console.log("✅ All tasks executed.");
            ws.close();
          }
          break;

        case "info":
          console.log(`ℹ️ Info: ${result}`);
          break;

        case "error":
          console.error(`❌ Orchestrator error: ${result}`);
          break;

        default:
          console.warn(`⚠️ Unknown message_type: ${message_type}`);
      }
    } catch (err) {
      console.error("❌ WebSocket message error:", err.message);
    }
  });

  ws.on("close", () => {
    console.log("🔌 WebSocket connection closed.");
    process.exit(0);
  });
  ws.on("error", (err) => {
    console.error("❌ WebSocket error:", err.message);
    process.exit(1);
  });
}
