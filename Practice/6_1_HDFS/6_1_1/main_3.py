import subprocess
import os
from pathlib import Path

# Пути в HDFS и внутри контейнера
hdfs_path = "/user/hdfs/testfile/big_file.csv"
local_path_in_container = "/tmp/big_file.csv"

# Имя контейнера NameNode
namenode = "6_1_1-namenode-1"

# Путь на хосте (Windows)
local_output_path = Path(r"F:\Data_engineering\Internship_nova_data\Practice\6_1_HDFS\6_1_1\big_file.csv")

# 1) Удаляем файл внутри контейнера (если был)
subprocess.run([
    "docker", "exec", namenode, "rm", "-f", local_path_in_container
], check=True)

# 2) Копируем файл из HDFS внутрь контейнера
subprocess.run([
    "docker", "exec", namenode, "hdfs", "dfs", "-get", hdfs_path, local_path_in_container
], check=True)

# 3) Копируем файл из контейнера на хост
subprocess.run([
    "docker", "cp", f"{namenode}:{local_path_in_container}", str(local_output_path)
], check=True)

# 4) Подтверждение
if local_output_path.exists():
    print(f"Файл успешно скопирован в: {local_output_path}")
    print(f"Размер: {local_output_path.stat().st_size:,} байт")
else:
    print("Ошибка: файл не найден на хосте.")
