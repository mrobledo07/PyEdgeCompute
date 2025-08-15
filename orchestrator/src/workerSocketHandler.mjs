// workers/socketHandler.mjs
// import { workers } from "./state.mjs";
// import { sortWorkers } from "./workerManager.mjs";
import { mapreduceTasks } from "./tasksMapReduce.mjs";
import { processTaskQueue, dispatchTask } from "./tasksDispatcher.mjs";
import workerRegistry from "./workerRegistry.mjs";
import taskQueue from "./taskQueue.mjs";
import clientRegistry from "./clientRegistry.mjs";

export function handleWorkerSocket(ws, workerId) {
  // const worker = workers.find((w) => w.worker_id === workerId);
  const worker = workerRegistry.getWorkerById(workerId);
  worker.ws = ws;
  processTaskQueue();

  ws.on("close", () => {
    // const worker = workers.find((w) => w.worker_id === workerId);
    const worker = workerRegistry.getWorkerById(workerId);
    if (worker && worker?.tasksAssignated.len > 0) {
      const requeuedTasks = [];
      for (const [taskId, clientId] of worker.tasksAssignated.map.entries()) {
        const regex = /-(mapper\d+|reducer[\w\d]*)$/;
        const match = taskId.match(regex);
        let taskObj = null;

        if (match) {
          // El sufijo ocupa todo el final => taskId es una subtarea, no la principal
          const mainTaskId = taskId.replace(regex, "");
          taskObj = clientRegistry.getClientSubTask(
            clientId,
            mainTaskId,
            taskId
          );
        } else {
          // No tiene sufijo => es la tarea principal
          taskObj = clientRegistry.getClientTask(clientId, taskId);
        }

        if (taskObj) {
          const requeuedTask = {
            taskId,
            clientId,
          };
          requeuedTasks.push(`${requeuedTask.clientId}:${requeuedTask.taskId}`);
          taskQueue.push(requeuedTask);
        } else {
        }
      }
    }
    workerRegistry.removeWorker(workerId);
    //workers = workers.filter((w) => w.worker_id !== workerId);
    //sortWorkers();
  });

  ws.on("message", (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch (e) {
      console.error("❌ Invalid JSON from worker:", data.toString());
      return;
    }

    // let cpuTime = parseFloat(msg.cpuTime) || 0;
    // let ioTime = parseFloat(msg.ioTime) || 0;

    //const worker = workers.find((w) => w.worker_id === workerId);
    const worker = workerRegistry.getWorkerById(workerId);

    const completed = workerRegistry.completeTaskOnWorker(workerId, msg.taskId);
    if (completed) {
    } else {
      return;
    }
    const cleanedTaskId = msg.taskId.replace(/-(mapper\d+|reducer\d*?)$/, "");

    const client = clientRegistry.getClient(msg.clientId);
    if (!client) {
      return;
    }
    const clientTask = clientRegistry.getClientTask(
      msg.clientId,
      cleanedTaskId
    );
    if (!clientTask) {
      return;
    }

    if (clientTask.state === "done" || clientTask.state === "error") {
      return;
    }

    // console.log(
    //   ">> task COMPLETED clients map keys:",
    //   Array.from(clientRegistry.clients.keys())
    // );

    let infoTask = mapreduceTasks.get(cleanedTaskId);

    if (infoTask && msg.status === "error") {
      mapreduceTasks.delete(cleanedTaskId); // Mapreduce task failed if any mapper or reducer fails
      const metadata = {
        [msg.taskId]: [
          parseFloat(msg.initTime) || 0,
          parseFloat(msg.readTime) || 0,
          parseFloat(msg.cpuTime) || 0,
          parseFloat(msg.writeTime) || 0,
          parseFloat(msg.endTime) || 0,
          msg.workerId || 0,
        ],
      };
      clientTask.subTasksResults.push(metadata);
    }

    infoTask = mapreduceTasks.get(cleanedTaskId);

    if (infoTask && infoTask.numMappers > 0) {
      infoTask.numMappers--;
      // const result = {
      //   result: msg.result,
      //   initTime,
      //   readTime,
      //   cpuTime,
      //   writeTime,
      //   endTime,
      // };
      const metadata = {
        [msg.taskId]: [
          parseFloat(msg.initTime) || 0,
          parseFloat(msg.readTime) || 0,
          parseFloat(msg.cpuTime) || 0,
          parseFloat(msg.writeTime) || 0,
          parseFloat(msg.endTime) || 0,
          msg.workerId || 0,
        ],
      };
      infoTask.resultsMappers.push(msg.result);
      clientTask.subTasksResults.push(metadata);
      //mapreduceTasks.set(cleanedTaskId, infoTask);
    }

    //infoTask = mapreduceTasks.get(msg.taskId);
    if (infoTask && infoTask.numMappers === 0) {
      infoTask.numMappers = -1;
      //mapreduceTasks.set(msg.taskId, infoTask);

      const type =
        infoTask.type === "mapwordcount" ? "reducewordcount" : "reduceterasort";

      // clientRegistry.addTask(msg.clientId, {
      //   code: infoTask.codeReduce,
      //   arg: infoTask.results,
      //   taskId: msg.taskId,
      //   type,
      //   numReducers: infoTask.numReducers,
      //   numMappers: infoTask.numMappers,
      // });

      const reduceTask = {
        code: infoTask.codeReduce,
        arg: infoTask.resultsMappers,
        taskId: cleanedTaskId,
        clientId: msg.clientId,
        type,
        numReducers: infoTask.numReducers,
        numMappers: infoTask.numMappers,
      };

      // const clientTask = clientRegistry.getClientTask(
      //   msg.clientId,
      //   cleanedTaskId
      // );
      // clientTask.stopwatch.stop(); // Stop stopwatch for the task
      // clientTask.executionTime += clientTask.stopwatch.getDuration();
      // infoTask.results = []; // Clear results for the next phase

      const dispatched = dispatchTask(reduceTask);
      if (!dispatched) {
        mapreduceTasks.delete(cleanedTaskId);
        msg.status = "error";
        msg.result = `Failed to dispatch reduce task for ${cleanedTaskId}. Task removed.`;
      } else {
        return; // Exit early if reduce task was dispatched successfully
      }
    }

    // console.log(
    //   ">> task COMPLETED clients map keys after map stage:",
    //   Array.from(clientRegistry.clients.keys())
    // );

    // console.log("!! tasks in mapreduceTasks:", mapreduceTasks.size);
    // console.log(">> mapreducetasks:", Array.from(mapreduceTasks.keys()));

    infoTask = mapreduceTasks.get(cleanedTaskId);
    let results = [];
    //infoTask = mapreduceTasks.get(cleanedTaskId);
    if (infoTask && infoTask.numMappers === -1) {
      infoTask.numReducers--;
      // const result = {
      //   result: msg.result,
      //   initTime,
      //   readTime,
      //   cpuTime,
      //   writeTime,
      //   endTime,
      // };
      const metadata = {
        [msg.taskId]: [
          parseFloat(msg.initTime) || 0,
          parseFloat(msg.readTime) || 0,
          parseFloat(msg.cpuTime) || 0,
          parseFloat(msg.writeTime) || 0,
          parseFloat(msg.endTime) || 0,
          msg.workerId || 0,
        ],
      };
      infoTask.resultsReducers.push(msg.result);
      clientTask.subTasksResults.push(metadata);
      if (infoTask.numReducers === 0) {
        // const clientTask = clientRegistry.getClientTask(
        //   msg.clientId,
        //   cleanedTaskId
        // );
        //clientTask.stopwatch.stop(); // Stop stopwatch for the task
        // clientTask.executionTime = parseFloat(
        //   (
        //     clientTask.executionTime + clientTask.stopwatch.getDuration()
        //   ).toFixed(4)
        // );
        results = infoTask.resultsReducers;
        // cpuTime =
        //   Math.max(...infoTask.resultsReducers.map((r) => r[1])) +
        //   Math.max(...infoTask.resultsMappers.map((r) => r[1]));
        // ioTime =
        //   Math.max(...infoTask.resultsReducers.map((r) => r[2])) +
        //   Math.max(...infoTask.resultsMappers.map((r) => r[2]));

        mapreduceTasks.delete(cleanedTaskId);
      } else {
      }
    }

    // console.log(
    //   ">> task COMPLETED clients map keys before final:",
    //   Array.from(clientRegistry.clients.keys())
    // );
    // console.log("!! tasks in mapreduceTasks:", mapreduceTasks.size);
    // console.log(">> mapreducetasks:", Array.from(mapreduceTasks.keys()));

    if (results.length === 0) {
      results = msg.result; // If no reducers, use the result from the message
      // const clientTask = clientRegistry.getClientTask(
      //   msg.clientId,
      //   cleanedTaskId
      // );
      // clientTask.executionTime = parseFloat(
      //   (clientTask.executionTime + clientTask.stopwatch.getDuration()).toFixed(
      //     4
      //   )
      // );
    }
    // else {
    //   results = results.map((r) => r.result); // Extract only the result part
    //   // console.log("🔍 results:", results);
    //   // console.log("🔍 results length:", results.length);
    //   // console.log("🔍 results[0]:", results[0]);
    //   // console.log("🔍 results[0] type:", typeof results[0]);
    // }
    infoTask = mapreduceTasks.get(cleanedTaskId);

    if (!infoTask) {
      if (msg.status === "done") {
        // console.log(
        //   ">> clients map keys in markdone:",
        //   Array.from(clientRegistry.clients.keys())
        // );
        clientRegistry.markTaskDone(msg.clientId, msg.taskId);
      } else if (msg.status === "error") {
        clientRegistry.markTaskError(msg.clientId, cleanedTaskId, msg.result);
      }

      // console.log(
      //   ">> task COMPLETED clients map keys final:",
      //   Array.from(clientRegistry.clients.keys())
      // );
      // console.log("!! tasks in mapreduceTasks:", mapreduceTasks.size);
      // console.log(">> mapreducetasks:", Array.from(mapreduceTasks.keys()));
      // console.log("!! tasks in mapreduceTasks:", mapreduceTasks.size);
      // console.log(">> mapreducetasks:", Array.from(mapreduceTasks.keys()));
      //const client = taskClients.get(msg.taskId);
      //const client = clientRegistry.getClient(msg.clientId);
      // const clientTask = clientRegistry.getClientTask(
      //   msg.clientId,
      //   cleanedTaskId
      // );
      // const initTime = parseFloat(msg.initTime) || 0;
      // const readTime = parseFloat(msg.readTime) || 0;
      // const cpuTime = parseFloat(msg.cpuTime) || 0;
      // const writeTime = parseFloat(msg.writeTime) || 0;
      // const endTime = parseFloat(msg.endTime) || 0;
      // metadata = {
      //   initTime,
      //   readTime,
      //   cpuTime,
      //   writeTime,
      //   endTime,
      // };
      let metadata, jsonToSend;
      if (clientTask.subTasksResults.length > 0) {
        metadata = clientTask.subTasksResults.reduce((acc, curr) => {
          return { ...acc, ...curr };
        }, {});
        //metadata = JSON.stringify(metadata);
        //console.log("WE ARE IN MAPREDUCE JSONTOSEND:", metadata);
        jsonToSend = {
          message_type: "task_result",
          type: clientTask.type,
          numMappers: clientTask.numMappers,
          numReducers: clientTask.numReducers,
          taskId: cleanedTaskId,
          status: msg.status,
          result: results,
          metadata,
        };
        //console.log("JSONTOSEND:", jsonToSend);

        //console.log("METADATA MAPREDUCE:", metadata);
      } else {
        metadata = {
          [msg.taskId]: [
            parseFloat(msg.initTime) || 0,
            parseFloat(msg.readTime) || 0,
            parseFloat(msg.cpuTime) || 0,
            parseFloat(msg.writeTime) || 0,
            parseFloat(msg.endTime) || 0,
          ],
        };
        //metadata = JSON.stringify(metadata);
        //console.log("WE ARE IN NOO !! MAPREDUCE JSONTOSEND:", metadata);
        jsonToSend = {
          message_type: "task_result",
          type: clientTask.type,
          taskId: cleanedTaskId,
          status: msg.status,
          result: results,
          metadata,
        };
        //console.log("JSONTOSEND:", jsonToSend);
        //console.log("METADATA:", metadata);
      }

      if (client?.ws) {
        client.ws.send(JSON.stringify(jsonToSend));
        clientRegistry.setClientTask(msg.clientId, cleanedTaskId, {
          ...clientTask,
          ...jsonToSend,
          sent: true,
        });

        if (clientRegistry.allTasksExecuted(msg.clientId)) {
          const clientTasks = clientRegistry.getClientTasks(msg.clientId);
          const taskIds = clientTasks.map((task) => task.taskId);
          taskQueue.remove(taskIds); // remove in case tasks are still pending
          clientRegistry.removeClient(msg.clientId);

          client.ws.close();
        }
      } else {
        clientRegistry.setClientTask(msg.clientId, cleanedTaskId, {
          ...clientTask,
          ...jsonToSend,
          sent: false,
        });
      }
    }

    processTaskQueue();
  });
}
