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

        # 检查远程目录是否存在，如果不存在则创建
        try:
            sftp.stat(remote_directory)
        except FileNotFoundError:
            sftp.mkdir(remote_directory)

        print("uploading file: " + local_path)
        # 上传文件
        sftp.put(local_path, remote_path)

        # 在远程服务器上创建.done文件
        done_file_remote_path = remote_path + '.done'
        sftp.open(done_file_remote_path, 'w').close()
        print("uploading file done: " + local_path)

    finally:
        # 关闭SFTP客户端和SSH客户端的连接
        if sftp is not None:
            sftp.close()
        client.close()


def process_json_file(efile_path):
    with open(efile_path, 'r') as file:
        for line in file:
            if line.strip():  # 确保行非空
                obj = json.loads(line)
                if all(field in obj for field in required_fields):
                    a_data = {
                        "wid": obj.get("id", "NULL"),
                        "doi": obj.get("doi", "NULL"),
                        "title": obj.get("title", "NULL"),
                        "display_name": obj.get("display_name", "NULL"),
                        "publication_year": obj.get("publication_year", "NULL"),
                        "publication_date": obj.get("publication_date", "NULL"),
                        "language": obj.get("language", "NULL"),
                        "primary_location": obj.get("primary_location", "NULL"),
                        "type": obj.get("type", "NULL"),
                        "authorships": obj.get("authorships", "NULL"),
                        "countries_distinct_count": obj.get("countries_distinct_count", "NULL"),
                        "institutions_distinct_count": obj.get("institutions_distinct_count", "NULL"),
                        "cited_by_count": obj.get("cited_by_count", "NULL"),
                        "keywords": obj.get("keywords", "NULL"),  # 设置默认值为 "NULL"
                        "referenced_works_count": obj.get("referenced_works_count", "NULL"),
                        "referenced_works": obj.get("referenced_works", "NULL"),
                        "related_works": obj.get("related_works", "NULL"),
                        "counts_by_year": obj.get("counts_by_year", "NULL"),
                        "updated_date": obj.get("updated_date", "NULL"),
                        "created_date": obj.get("created_date", "NULL"),
                    }
                    yield a_data
                else:
                    # 如果对象缺少必需字段，则跳过此对象
                    continue


def write_json_data(ggenerator, output_file):
    with open(output_file, 'w') as file:
        for item in ggenerator:
            json.dump(item, file)
            file.write('\n')


# 按装订区域中的绿色按钮以运行脚本。
if __name__ == '__main__':
    current_dir = os.getcwd()
    # test_au_dir = "I:\\openalex\\works"
    # test_au_dir = "authors"
    test_au_dir = "E:\\openalex-snapshot\\data\\works"
    required_fields = ['title']
    for root, dirs, files in os.walk(test_au_dir):
        for file_name in files:
            if file_name.endswith('.gz'):
                file_path = os.path.join(root, file_name)
                print(root)
                print(file_name)
                extract_file_path = os.path.splitext(file_path)[0]
                print(extract_file_path)
                # 解压缩文件
                with gzip.open(file_path, 'rb') as gz_file:
                    with open(extract_file_path, 'wb') as extracted_file:
                        extracted_file.write(gz_file.read())

                directory_name = []
                print("begin to process json file:" + extract_file_path)
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
                print("process json file done:" + extract_file_path)

                print("begin to write json data:" + json_output_file)
                write_json_data(generator, json_output_file)
                print("write json data done:" + json_output_file)

                # 上传至服务器
                remote_file_path = "/home/sa/Data-Script/works/" + os.path.basename(json_output_file)
                upload_file(json_output_file, remote_file_path, "116.63.49.180", 22, "sa", "@buaa-sa-13")

                # 清理本地文件
                os.remove(extract_file_path)
                os.remove(json_output_file)

