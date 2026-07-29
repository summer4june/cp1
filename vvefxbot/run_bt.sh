#!/bin/bash
export PATH="/Users/Vikas/Documents/cp1/vvefxbot/vvefxbot_env/bin:/usr/local/bin:/usr/bin:/bin"
unset CONDA_PREFIX
unset CONDA_DEFAULT_ENV
python -u backtest.py > bt_out.log 2>&1
echo "BACKTEST DONE" >> bt_out.log
