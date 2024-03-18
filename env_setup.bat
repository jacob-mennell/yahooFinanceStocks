@echo off

set VENV_NAME=stock_env
set ENV_FILE=stock_env.yml

if not exist %VENV_NAME% (
    conda env create --name %VENV_NAME% --file %ENV_FILE%
)

call conda activate %VENV_NAME%

echo Environment setup complete.