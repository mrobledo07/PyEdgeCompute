// tasks/dispatcher.mjs

import { mapreduceTasks } from "./tasksMapReduce.mjs";
import workerRegistry from "./workerRegistry.mjs";
import taskQueue from "./taskQueue.mjs";
import clientRegistry from "./clientRegistry.mjs";

/**
 * Processes the task queue by trying to assign tasks to available workers.
 */
export function processTaskQueue() {
  while (
    taskQueue.size() > 0 &&
    workerRegistry.getTotalAvailableWorkers() > 0
  ) {
    const next = taskQueue.peek(); // Peek

    const { clientId, taskId } = next;
    const regex = /-(mapper\d+|reducer[\w\d]*)$/;

    const match = taskId.match(regex);

    let task = null;
    let mainTaskId = taskId;

    if (match) {
      // Subtask case
      mainTaskId = taskId.replace(regex, "");
      task = clientRegistry.getClientSubTask(clientId, mainTaskId, taskId);
    } else {
      // Main task case
      task = clientRegistry.getClientTask(clientId, taskId);
    }

    if (!task) {
      // Optional: remove from queue to avoid blocking
      taskQueue.shift();
      continue;
    }

    //const task = clientRegistry.getClientTask(next.clientId, next.taskId);
    const numAvailableWorkers = workerRegistry.getTotalAvailableWorkers();

    const canDispatch = (() => {
      // if (
      //   task.type === "mapreducewordcount" ||
      //   task.type === "mapreduceterasort"
      // ) {
      //   console.log(`Mappers needed: ${task.arg[1]}`);
      //   console.log(`Available workers: ${numAvailableWorkers}`);
      //   return numAvailableWorkers >= task.arg[1]; // num mappers
      // }

      // if (task.type === "reduceterasort") {
      //   console.log(`Reducers needed: ${task.numReducers}`);
      //   console.log(`Available workers: ${numAvailableWorkers}`);
      //   return numAvailableWorkers >= task.numReducers;
      // }

      return numAvailableWorkers > 0;
    })();

    if (!canDispatch) {
      break;
    }
    // Now remove the task from the queue
    taskQueue.shift();
    let taskInfo;
    taskInfo = {
      clientId,
      taskId,
      code: task.code,
      arg: task.arg,
      type: task.type,
    };

    if (match) {
      taskInfo = {
        ...taskInfo,
        numMappers: task.numMappers,
        numReducers: task.numReducers,
        numWorker: task.numWorker,
      };
    }

    if (taskInfo.type === "reduceterasort") {
      const isAlreadyMatrix =
        Array.isArray(task.arg) && Array.isArray(task.arg[0]);

      if (!isAlreadyMatrix) {
        taskInfo.arg = task.arg.map((el) => [el]);
      }
    }

    dispatchTask(taskInfo);
    //const { clientId, taskId } = taskQueue.shift(); // Remove from queue
    // const task = clientRegistry.getClientTask(clientId, taskId);
    // const taskInfo = {
    //   clientId,
    //   taskId,
    //   code: task.code,
    //   arg: task.arg,
    //   type: task.type,
    // };
    // dispatchTask(taskInfo);
  }
}

/**
 * Dispatches a single task depending on its type.
 */
export function dispatchTask(task) {
  try {
    if (
      task.type === "mapreducewordcount" ||
      task.type === "mapreduceterasort"
    ) {
      const ok = dispatchMappers(task);
      if (!ok) taskQueue.push({ clientId: task.clientId, taskId: task.taskId });
      return true;
    }

    const regex = /-(mapper\d+|reducer[\w\d]*)$/;

    const match = task.taskId.match(regex);

    // only dispatch reducers again if is a new reduce job
    if (
      (task.type === "reducewordcount" || task.type === "reduceterasort") &&
      !match
    ) {
      const ok = dispatchReducers(task);
      if (!ok) taskQueue.push({ clientId: task.clientId, taskId: task.taskId });
      return true;
    }

    // const oldTaskId = task.taskId; // Save original taskId
    // if (task.type === "reducewordcount") {
    //   task.taskId = `${task.taskId}-reducer`;
    // }

    // Default case: normal task
    const worker = workerRegistry.getBestWorkers(1)[0];
    if (worker) {
      if (Array.isArray(task.code) && task.code.length > 1) {
        task.code = task.code[0]; // Use first code block
      }
      //clientRegistry.getClientTask(task.clientId, oldTaskId).stopwatch.start();
      reserveWorkerAndSendTask(worker, task);
    } else {
      taskQueue.push({ clientId: task.clientId, taskId: task.taskId });
    }
    return true;
  } catch (err) {
    return false; // Indicate failure
  } // opcional: stack trace completa}
}

/**
 * Dispatches map tasks to available workers.
 */
function dispatchMappers(task) {
  //const numAvailableWorkers = workerRegistry.getTotalAvailableWorkers();
  const [mapperCode, reducerCode] = task.code;
  const numMappers = task.arg[1];

  // if (numMappers > numAvailableWorkers) {
  //   console.log(
  //     `🕒 Not enough workers for map phase of task ${task.taskId} from client ${task.clientId}`
  //   );
  //   console.log("Required mappers:", numMappers);
  //   console.log("Available workers:", numAvailableWorkers);
  //   return false;
  // }

  // Prepare mapper task
  task.code = mapperCode;
  task.numMappers = numMappers;

  if (task.type === "mapreducewordcount") {
    task.type = "mapwordcount";
    task.numReducers = 1;
  } else {
    const numReducers = task.arg[2];
    task.type = "mapterasort";
    task.numReducers = numReducers;
  }

  task.arg = task.arg[0]; // raw args to send to each mapper
  const workersAvailable = workerRegistry.getBestWorkers(numMappers);
  // Dispatch to N mappers
  mapreduceTasks.set(task.taskId, {
    numMappers: task.numMappers,
    numReducers: task.numReducers,
    codeReduce: reducerCode,
    type: task.type,
    resultsMappers: [],
    resultsReducers: [],
  });
  for (let i = 0; i < numMappers; i++) {
    //const worker = workersAvailable[i];
    const isAssigned = i < workersAvailable.length;
    const worker = isAssigned ? workersAvailable[i] : null;
    const individualTask = {
      ...task,
      taskId: `${task.taskId}-mapper${i}`,
      numWorker: isAssigned ? worker.worker_num : null,
    };
    clientRegistry.addSubTask(task.clientId, task.taskId, individualTask);
    const clientTask = clientRegistry.getClientTask(task.clientId, task.taskId);
    const taskClient = {
      ...clientTask,
      numMappers: task.numMappers,
      numReducers: task.numReducers,
    };
    clientRegistry.setClientTask(task.clientId, task.taskId, taskClient);
    // const clientTask2 = clientRegistry.getClientTask(
    //   task.clientId,
    //   task.taskId
    // );
    //console.log("CLIENT TASK:", clientTask2);
    //clientRegistry.addTask(`${task.taskId}-mapper${i}`, individualTask);

    if (isAssigned) {
      // Send to worker i
      reserveWorkerAndSendTask(worker, individualTask);
    } else {
      // No worker now → enqueue for later
      taskQueue.push({
        clientId: task.clientId,
        taskId: individualTask.taskId,
      });
    }
    //reserveWorkerAndSendTask(worker, individualTask);
  }

  // clientRegistry.getClientTask(task.clientId, task.taskId).stopwatch.start(); // Start stopwatch for the task
  return true;
}

/**
 * Dispatches reduce tasks to available workers.
 */
function dispatchReducers(task) {
  //const numAvailableWorkers = workerRegistry.getTotalAvailableWorkers();
  const numReducers = task.numReducers;

  // if (numReducers > numAvailableWorkers) {
  //   console.log(
  //     `🕒 Not enough workers for reduce phase of task ${task.taskId} from client ${task.clientId}`
  //   );
  //   console.log("Required reducers:", numReducers);
  //   console.log("Available workers:", numAvailableWorkers);
  //   return false;
  // }
  // task.arg is in the format [ [ “…_0.txt”, “…_1.txt”, … ],  // mapper 0
  //                         [ “…_0.txt”, “…_1.txt”, … ],     // mapper 1
  //                         …                          ]    // mapper m

  const workersAvailable = workerRegistry.getBestWorkers(numReducers);

  if (task.type === "reduceterasort") {
    const argsMatrix = task.arg; // [ [m0_files], [m1_files], ... ]

    for (let r = 0; r < numReducers; r++) {
      const reducerArgs = [];
      const isAssigned = r < workersAvailable.length;
      const worker = isAssigned ? workersAvailable[r] : null;

      for (const [i, mapperFiles] of argsMatrix.entries()) {
        if (!Array.isArray(mapperFiles)) {
          throw new Error(
            `Invalid input: argsMatrix[${i}] is not an array. Got: ${typeof mapperFiles}`
          );
        }
        const file = mapperFiles.find((f) => f.endsWith(`-${r}.txt`));
        if (!file) {
          throw new Error(`Reducer ${r} missing input from mapper.`);
        }
        reducerArgs.push(file);
      }

      const reducerTask = {
        ...task,
        taskId: `${task.taskId}-reducer${r}`,
        arg: reducerArgs,
        numWorker: isAssigned ? worker.worker_num : null,
      };
      clientRegistry.addSubTask(task.clientId, task.taskId, reducerTask);

      //clientRegistry.addTask(`${task.taskId}-reducer${r}`, reducerTask);
      if (isAssigned) {
        // assign to available worker r
        reserveWorkerAndSendTask(workersAvailable[r], reducerTask);
      } else {
        // no free worker → enqueue
        taskQueue.push({ clientId: task.clientId, taskId: reducerTask.taskId });
      }
      //reserveWorkerAndSendTask(workersAvailable[r], reducerTask);
    }
  } else {
    const reducerTask = {
      ...task,
      taskId: `${task.taskId}-reducer`,
    };
    //clientRegistry.addTask(`${task.taskId}-reducer`, reducerTask);
    reserveWorkerAndSendTask(workersAvailable[0], reducerTask);
  }

  // clientRegistry.getClientTask(task.clientId, task.taskId).stopwatch.start(); // Start stopwatch for the task

  return true;
}

/**
 * Assigns a task to a worker and sends it via WebSocket.
 */
function reserveWorkerAndSendTask(worker, task) {
  workerRegistry.assignTaskToWorker(
    worker.worker_id,
    task.clientId,
    task.taskId
  ); // Decrement first
  clientRegistry.markTaskRunning(task.clientId, task.taskId, worker.worker_id);
  worker.ws.send(JSON.stringify(task), (err) => {
    if (err) {
      workerRegistry.completeTaskOnWorker(worker.worker_id, task.taskId); // Increment back (as if completed/failed)
      taskQueue.push({ clientId: task.clientId, taskId: task.taskId }); // Re-queue
      clientRegistry.markTaskPending(task.clientId, task.taskId);
      throw new Error(
        `Failed to send task to worker ${worker.worker_id}. Error: ${err.message}`
      );
    } else {
    }
  });
}
