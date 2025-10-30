set -e
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "$DIR/venv/bin/activate"


uvicorn mongo_db_interaction.api:app --reload
