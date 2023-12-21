#/bin/bash

# 获取当前系统时间，并格式化为LOG+时间.log
current_time=$(date +"%Y-%m-%d_%H-%M-%S")
log_filename="LOG/LOG+$current_time.log"

# 使用nohup启动Django runserver并将日志输出到指定文件
nohup python3 -u authors.py > output1.log 2>&1 &
nohup python3 -u works.py > output2.log 2>&1 &

echo "Server restarted and logs will be saved to $output.log"
