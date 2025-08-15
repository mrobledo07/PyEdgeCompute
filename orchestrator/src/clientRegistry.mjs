class ClientRegistry {
  constructor() {
    if (ClientRegistry.instance) return ClientRegistry.instance;
    this.clients = new Map(); // clientId -> { ws, numTasks, tasks: Map }
    this.isLocked = false;
    ClientRegistry.instance = this;
  }

  lock() {
    if (this.isLocked) throw new Error("ClientRegistry is locked.");
    this.isLocked = true;
  }

  unlock() {
    this.isLocked = false;
  }

  allTasksExecuted(clientId) {
    const client = this.clients.get(clientId);
    if (!client) return false;
    return client.numPendingTasks === 0;
  }

  registerClient(clientId, ws, numTasks) {
    this.lock();
    try {
      this.clients.set(clientId, {
        ws,
        numTasks,
        numPendingTasks: numTasks,
        tasks: new Map(),
      });
      console.log(`REGISTERED CLIENT ${clientId} with ${numTasks} tasks`);
    } finally {
      this.unlock();
    }
  }

  removeClient(clientId) {
    this.lock();
    try {
      this.clients.delete(clientId);
    } finally {
      this.unlock();
    }
  }

  getAllClients() {
    if (this.isLocked) throw new Error("ClientRegistry is locked.");
    return Array.from(this.clients.entries()).map(([clientId, client]) => ({
      clientId,
      numTasks: client.numTasks,
      tasks: Array.from(client.tasks.entries()).map(([taskId, info]) => ({
        taskId,
        code: info.code,
        args: info.arg,
        type: info.type,
        state: info.state,
        assignedWorkers: Array.from(info.assignedWorkers.keys()),
        error: info.error || null,
      })),
    }));
  }

  getClient(clientId) {
    if (this.isLocked) throw new Error("ClientRegistry is locked.");
    return this.clients.get(clientId);
  }

  getClientTasks(clientId) {
    if (this.isLocked) throw new Error("ClientRegistry is locked.");
    const client = this.clients.get(clientId);
    return Array.from(client.tasks.entries()).map(([taskId, info]) => ({
      taskId,
      clientId,
      ...info, // Aquí se incluye toda la información de la tarea
    }));
  }

  addTask(clientId, task) {
    this.lock();
    try {
      const client = this.clients.get(clientId);
      client.numTasks++;
      client.numPendingTasks++;
      client.tasks.set(task.taskId, {
        code: task.code,
        arg: task.arg,
        type: task.type,
        state: "pending",
        assignedWorkers: new Map(),
        subTasksResults: [],
        subTasks: [],
        //stopwatch: task.stopwatch,
        //executionTime: task.executionTime || 0,
      });
      console.log(`Added task ${task} to client ${clientId}`);
      console.log(client.tasks);
    } finally {
      this.unlock();
    }
  }

  addSubTask(clientId, taskId, subTask) {
    this.lock();
    try {
      const client = this.clients.get(clientId);
      if (!client) {
        console.error(`❌ Client ${clientId} not found`);
        return;
      }

      const mainTask = client.tasks.get(taskId);
      if (!mainTask) {
        console.error(`❌ Task ${taskId} not found for client ${clientId}`);
        return;
      }

      // Añadir la subtarea al array
      mainTask.subTasks.push(subTask);
      console.log(
        `➕ Added subtask ${subTask.taskId} to ${taskId} of client ${clientId}`
      );
      console.log(mainTask.subTasks);
    } finally {
      this.unlock();
    }
  }

  markTaskRunning(clientId, taskId, workerId) {
    this.lock();
    try {
      const task = this.clients.get(clientId).tasks.get(taskId);
      if (!task) return;
      task.state = "running";
      task.assignedWorkers.set(workerId, Date.now());
    } finally {
      this.unlock();
    }
  }

  markTaskPending(clientId, taskId) {
    this.lock();
    try {
      const task = this.clients.get(clientId).tasks.get(taskId);
      if (!task) return;
      task.state = "pending";
    } finally {
      this.unlock();
    }
  }

  markTaskDone(clientId, taskId) {
    this.lock();
    try {
      const client = this.clients.get(clientId);
      const task = client.tasks.get(taskId);
      if (!task) return;
      task.state = "done";
      client.numPendingTasks--;
    } finally {
      this.unlock();
    }
  }

  markTaskError(clientId, taskId, errorMsg) {
    this.lock();
    try {
      const client = this.clients.get(clientId);
      const task = client.tasks.get(taskId);
      if (!task) return;
      task.state = "error";
      task.error = errorMsg;
      client.numPendingTasks--;
    } finally {
      this.unlock();
    }
  }

  getClientStatus(clientId) {
    if (this.isLocked) throw new Error("ClientRegistry is locked.");
    const client = this.clients.get(clientId);
    return {
      numTasks: client.numTasks,
      tasks: Array.from(client.tasks.entries()).map(([taskId, info]) => ({
        taskId,
        state: info.state,
        assignedWorkers: Array.from(info.assignedWorkers.keys()),
        error: info.error || null,
      })),
    };
  }

  getClientTask(clientId, taskId) {
    if (this.isLocked) throw new Error("ClientRegistry is locked.");
    const client = this.clients.get(clientId);
    if (!client) return null;
    return client.tasks.get(taskId);
  }

  getClientSubTask(clientId, taskId, subTaskId) {
    if (this.isLocked) throw new Error("ClientRegistry is locked.");

    console.log("CLIENT HELLO");
    const client = this.clients.get(clientId);
    if (!client) return null;
    console.log(client);

    console.log("TASK HELLO");
    const mainTask = client.tasks.get(taskId);
    if (!mainTask) return null;
    console.log(mainTask);

    console.log("TRYING TO FIND SUBTASK");
    console.log(mainTask.subTasks);
    return (
      mainTask.subTasks.find((subTask) => subTask.taskId === subTaskId) || null
    );
  }

  setClientTask(clientId, taskId, taskInfo) {
    this.lock();
    try {
      const client = this.clients.get(clientId);
      if (!client) return;
      client.tasks.set(taskId, taskInfo);
    } finally {
      this.unlock();
    }
  }
}

const instance = new ClientRegistry();
export default instance;
