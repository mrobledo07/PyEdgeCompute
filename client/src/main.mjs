// client.js
import { obtainArgs } from "./utils.mjs";
import { loadConfig, loadCode } from "./configLoader.mjs";
import {
  sendTaskWithRetry,
  connectToWebSocket,
  //connectClient,
} from "./orchestratorAPI.mjs";
import { Stopwatch } from "./stopWatch.mjs";
const CONFIG = {
  HTTP_ORCH: null,
  WS_ORCH: null,
  CONFIG_PATH: null,
};

async function main() {
  obtainArgs(CONFIG);

  let task, maxTasks;
  try {
    const config = await loadConfig(CONFIG.CONFIG_PATH);
    const code = await loadCode(config.code);
    maxTasks = config.args.length;
    task = {
      type: config.type,
      args: config.args,
      code: code,
    };
  } catch (err) {
    console.error("❌ Config error:", err.message);
    process.exit(1);
  }

  const clientId = await sendTaskWithRetry(task, CONFIG.HTTP_ORCH);
  let stopwatches = [];
  for (let i = 0; i < maxTasks; i++) {
    const stopwatch = new Stopwatch();
    stopwatch.start();
    stopwatches.push(stopwatch);
  }
  console.log("🕒 Stopwatch started for task:", clientId);
  const sentTime = Date.now() / 1000; // Convert to seconds
  connectToWebSocket(
    CONFIG.WS_ORCH,
    clientId,
    maxTasks,
    stopwatches,
    sentTime,
    CONFIG.OUT_DIR
  );
}

main();
