#!/bin/bash

ANALYSIS_SCRIPT="analyze_mapreduce.py"

# Verifica que el script de análisis existe
if [ ! -f "$ANALYSIS_SCRIPT" ]; then
  echo "Error: $ANALYSIS_SCRIPT no encontrado en el directorio actual."
  exit 1
fi

# Recorre todos los subdirectorios (menos pyenv)
for dir in */ ; do
  dir=${dir%/}
  if [ "$dir" == "pyenv" ]; then
    continue
  fi

  echo "📁 Analizando directorio: $dir"

  # Llama al script de análisis Python
  python "$ANALYSIS_SCRIPT" --results-dir "$dir"
done
