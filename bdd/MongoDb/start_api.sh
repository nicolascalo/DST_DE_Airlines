#!/bin/bash

cd "$(dirname "$0")"

source venv/bin/activate



export PYTHONPATH="$PWD:$PYTHONPATH"


uvicorn mongo_db_interaction.api:app --reload
