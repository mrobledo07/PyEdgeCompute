// utils.js
function parseArgs() {
  const args = process.argv.slice(2);
  const result = {};
  for (let i = 0; i < args.length; i++) {
    switch (args[i]) {
      case "--config":
        result.configPath = args[++i];
        break;
      case "--orch":
        result.orchestrator = args[++i];
        break;
      case "--client_id":
        result.client_id = args[++i];
        break;
      case "--outdir":
        result.outdir = args[++i];
        break;
    }
  }
  return result;
}

function convertHttpToWs(urlStr) {
  const url = new URL(urlStr);
  url.protocol = "ws";
  return url;
}

function isValidURL(url) {
  try {
    const newurl = new URL(url);
    if (newurl.protocol !== "http:" && newurl.protocol !== "https:")
      throw new Error();
    return true;
  } catch (_) {
    return false;
  }
}

function getOrchestratorUrls(input) {
  if (!isValidURL(input)) {
    console.error("Invalid orchestrator URL");
    process.exit(1);
  }

  const url = new URL(input);

  const hostRegex = /^([a-zA-Z0-9.-]+|\d{1,3}(\.\d{1,3}){3}):\d{1,5}$/;
  if (!hostRegex.test(url.host)) {
    throw new Error("Invalid host format. Expected: <domain|ip>:port");
  }

  let ws = convertHttpToWs(url.toString());
  return {
    http: url.protocol + "//" + url.host,
    ws: ws.protocol + "//" + ws.host,
  };
}

export function obtainArgs(CONFIG) {
  const { configPath, orchestrator, client_id, outdir } = parseArgs();

  if (client_id) {
    console.log("FUNCTIONALITY NOT IMPLEMENTED YET");
    return;
    // console.log("🔌 Using provided client ID:", client_id);
    // try {
    //   const res = await connectClient(client_id, urls.http);
    //   connectToWebSocket(urls.ws, client_id);
    // } catch (err) {}
  }

  if (!configPath || !orchestrator) {
    console.error("❌ Missing --config or --orch argument");
    process.exit(1);
  }

  let urls;
  try {
    urls = getOrchestratorUrls(orchestrator);
  } catch (err) {
    console.error("❌", err.message);
    process.exit(1);
  }

  CONFIG.HTTP_ORCH = urls.http;
  CONFIG.WS_ORCH = urls.ws;
  CONFIG.CONFIG_PATH = configPath;
  CONFIG.OUT_DIR = outdir || null;
  console.log(urls);
}
