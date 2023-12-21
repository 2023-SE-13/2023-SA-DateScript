# 这是一个示例 Python 脚本。
import pandas as pd
import os
import json
import gzip
import paramiko


def upload_file(local_path, remote_path, hostname, port, username, password):
    # 创建SSH客户端
    client = paramiko.SSHClient()
    sftp = None  # 初始化 sftp 对象

    try:
        # 自动添加服务器的主机密钥到本地的known_hosts文件中
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # 连接服务器
        client.connect(hostname=hostname, port=port, username=username, password=password)

        # 创建SFTP客户端
        sftp = client.open_sftp()

        # 获取远程目录路径和文件名
        remote_directory = remote_path.rsplit('/', 1)[0]
        remote_filename = remote_path.rsplit('/', 1)[1]
        print(remote_directory)
        print(remote_filename)
        # 检查远程目录是否存在，如果不存在则创建
        try:
            sftp.stat(remote_directory)
        except FileNotFoundError:
            sftp.mkdir(remote_directory)

        # 上传文件
        sftp.put(local_path, remote_path)

        done_file_remote_path = remote_path + '.done'
        sftp.open(done_file_remote_path, 'w').close()

    finally:
        # 关闭SFTP客户端和SSH客户端的连接
        if sftp is not None:
            sftp.close()
        client.close()

def process_json_file(efile_path):
    with open(efile_path, 'r') as file:
        for line in file:
            if line.strip():  # 确保行非空
                data = json.loads(line)
                a_data = {
                    "wid": data.get("id", "NULL"),
                    "doi": data.get("doi", "NULL"),
                    "title": data.get("title", "NULL"),
                    "display_name": data.get("display_name", "NULL"),
                    "publication_year": data.get("publication_year", "NULL"),
                    "publication_date": data.get("publication_date", "NULL"),
                    "language": data.get("language", "NULL"),
                    "primary_location": data.get("primary_location", "NULL"),
                    "type": data.get("type", "NULL"),
                    "authorships": data.get("authorships", "NULL"),
                    "countries_distinct_count": data.get("countries_distinct_count", "NULL"),
                    "institutions_distinct_count": data.get("institutions_distinct_count", "NULL"),
                    "cited_by_count": data.get("cited_by_count", "NULL"),
                    "keywords": data.get("keywords", "NULL"),  # 设置默认值为 "NULL"
                    "referenced_works_count": data.get("referenced_works_count", "NULL"),
                    "referenced_works": data.get("referenced_works", "NULL"),
                    "related_works": data.get("related_works", "NULL"),
                    "counts_by_year": data.get("counts_by_year", "NULL"),
                    "updated_date": data.get("updated_date", "NULL"),
                    "created_date": data.get("created_date", "NULL"),
                }
                yield a_data


def write_json_data(ggenerator, output_file):
    with open(output_file, 'w') as file:
        file.write('[')
        first = True
        for item in ggenerator:
            if not first:
                file.write(',')
            json.dump(item, file, indent=4)
            first = False
        file.write(']')

def read_file_lines(filename):
    lines = []
    try:
        with open(filename, "r") as file:
            for line in file:
                lines.append(line.strip())
    except FileNotFoundError:
        print(f"文件 '{filename}' 不存在")
    return lines


# 按装订区域中的绿色按钮以运行脚本。
if __name__ == '__main__':
    current_dir = os.getcwd()
    test_au_dir = os.path.join(current_dir, "works")

    lines_array = read_file_lines("copied.txt")

    for root, dirs, files in os.walk(test_au_dir):
        for file_name in files:
            flag = 0
            directory_name = []
            # 检查文件是否是 .gz 压缩文件
            if file_name.endswith('.gz'):
                file_path = os.path.join(root, file_name)
                print(file_path)
                # 创建解压后的文件路径
                extract_file_path = os.path.splitext(file_path)[0]

                # 解压缩文件
                with gzip.open(file_path, 'rb') as gz_file:
                    with open(extract_file_path, 'wb') as extracted_file:
                        extracted_file.write(gz_file.read())

                directory_name = []
                # 处理JSON数据
                generator = process_json_file(extract_file_path)
                temp_file_path = extract_file_path
                for i in range(3):
                    # new_data.append(a_data)
                    directory_name.append(os.path.basename(temp_file_path))
                    # print(directory_name)
                    # 获取目录部分的路径
                    temp_file_path = os.path.dirname(temp_file_path)
                json_output_file = directory_name[2] + "_" + directory_name[1] + "_" + directory_name[0] + ".json"
                print(json_output_file)
                write_json_data(generator, json_output_file)

                if len(directory_name) != 0:
                    for string in lines_array:
                        if string == directory_name[1]:
                            flag = 1
                            break

                if flag == 1:
                    continue

                remote_file_path = "/home/sa/Data-Script/works/" + os.path.basename(json_output_file)
                upload_file(json_output_file, remote_file_path, "116.63.49.180", 22, "sa", "@buaa-sa-13")

                mem_copy = "copied"+directory_name[2]+".txt"
                with open(mem_copy, "a") as file:
                    file.write(directory_name[1])
                    file.write("\n")

                # 清理本地文件
                os.remove(extract_file_path)
                os.remove(json_output_file)
