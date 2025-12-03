#!/bin/bash

./venv/bin/python ./venv/bin/uvicorn API.api:app --host 0.0.0.0 --port 8000 --reload
