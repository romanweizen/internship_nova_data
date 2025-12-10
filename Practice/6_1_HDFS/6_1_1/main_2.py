import subprocess
import os

# Пути в HDFS
hdfs_path = "/user/hdfs/testfile/big_file.csv"

# Пути внутри контейнера
local_path_in_container = "/tmp/big_file.csv"

# Путь на хосте (Windows)
local_output_path = r"big_file_from_hdfs.csv"

# Имя твоего контейнера с NameNode:
namenode = "6_1_1-namenode-1"

# Удаляем старый файл внутри контейнера
subprocess.run([
    "docker", "exec", namenode, "rm", "-f", local_path_in_container
], check=True)

# Копируем файл из HDFS внутрь контейнера
subprocess.run([
    "docker", "exec", namenode, "hdfs", "dfs", "-get", hdfs_path, local_path_in_container
], check=True)

# Копируем файл из контейнера на хост
subprocess.run([
    "docker", "cp", f"{namenode}:{local_path_in_container}", local_output_path
], check=True)

# Читаем первые строки
with open(local_output_path, "r", encoding="utf-8") as f:
    for i, line in enumerate(f):
        print(line.strip())
        if i >= 10:
            break

print("\nФайл успешно извлечён на хост:", local_output_path)
