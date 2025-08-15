// clients/socketHandler.mjs

// import { taskClients } from "./state.mjs";
//import taskQueue from "./taskQueue.mjs";
import clientRegistry from "./clientRegistry.mjs";
import taskQueue from "./taskQueue.mjs";

function sendTaskToClient(task, ws) {
  if (!ws || ws.readyState !== ws.OPEN) {
    return;
  }

  let message;
  if (task.subTasksResults.length > 0) {
    message = {
      message_type: task.message_type,
      type: task.type,
      numMappers: task.numMappers,
      numReducers: task.numReducers,
      taskId: task.taskId,
      status: task.status,
      result: task.results,
      metadata: task.metadata,
    };
  } else {
    message = {
      message_type: task.message_type,
      type: task.type,
      taskId: task.taskId,
      status: task.status,
      result: task.results,
      metadata: task.metadata,
    };
  }

  try {
    ws.send(JSON.stringify(message));
    task.sent = true;
  } catch (error) {}
}

export function handleClientSocket(ws, clientId) {
  //taskClients.get(taskId).ws = ws;
  //const client = (clientRegistry.getClient(clientId).ws = ws);
  clientRegistry.getClient(clientId).ws = ws;

  const tasks = clientRegistry.getClientTasks(clientId);
  if (Array.isArray(tasks) && tasks.length > 0) {
    for (const task of tasks) {
      if (task.sent === false) {
        sendTaskToClient(task, ws); // Send task
      }
    }
  }

  ws.on("close", () => {
    // if (client?.numTasks > 0) {
    //   //taskQueue = taskQueue.filter((task) => task.taskId !== taskId);
    //   const clientTasks = clientRegistry.getClientTasks(clientId);
    //   const taskIds = clientTasks.map((task) => task.taskId);
    //   taskQueue.remove(taskIds);
    //   console.log(
    //     `🗑️ Tasks ${taskIds} removed from queue due to client disconnect.`
    //   );
    // }
    // clientRegistry.removeClient(clientId); // Remove client from registry
    if (clientRegistry.allTasksExecuted(clientId)) {
      const clientTasks = clientRegistry.getClientTasks(clientId);
      const taskIds = clientTasks.map((task) => task.taskId);
      taskQueue.remove(taskIds); // remove in case tasks are still pending
      clientRegistry.removeClient(clientId);
    }
    // Remove client from registry if all tasks executed
    // else
    //   console.log(
    //     `⚠️ Client ${clientId} disconnected but tasks still pending.`
    //   );
  });
}
