#!/bin/bash
cd /home/ec2-user

export NVM_DIR="/home/ec2-user/.nvm"
source "$NVM_DIR/nvm.sh"
# Para que puedas revisar logs y que no bloquee el arranque:
# nohup: permite que el proceso siga vivo tras cerrar la sesión o tras el arranque.
# > /var/log/my-node-app.log 2>&1: guarda stdout y stderr.
# &: ejecuta en background.
# Podriamos quitar el nohup y el &, asi init_script no acabaría mientras se esté ejecutando el worker
# pero esto hace que la inicialización no acabe nunca, pudiendo causar timeouts o errores de inicialización
# de manera que usamos & para que init_script pueda continuar con la ejecución
# y usamos nohup para que cuando el proceso shell de init_script acabe, no elimine el proceso hijo de node

nohup node src/main.mjs --orch http://ec2-13-48-55-235.eu-north-1.compute.amazonaws.com:3000 --storage s3://orchestratorfororchestratingworkers > /var/log/my-node-app.log 2>&1 &