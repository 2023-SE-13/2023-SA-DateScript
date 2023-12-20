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

        # 上传文件
        sftp.put(local_path, remote_path)

        # 在远程服务器上创建.done文件
        done_file_remote_path = remote_path + '.done'
        sftp.open(done_file_remote_path, 'w').close()

    finally:
        # 关闭SFTP客户端和SSH客户端的连接
        if sftp is not None:
            sftp.close()
        client.close()


# def process_json_file(efile_path):
#     with open(efile_path, 'rb') as file:
#         # 使用ijson解析器逐个处理JSON对象
#         objects = ijson.items(file, 'item')
#         for obj in objects:
#             a_data = {
#                 "aid": obj.get("aid"),
#                 "display_name": obj.get("display_name", "NULL"),
#                 "cited_by_count": obj.get("cited_by_count", "NULL"),
#                 "counts_by_year": obj.get("counts_by_year", "NULL"),
#                 "works_count": obj.get("works_count", "NULL"),
#                 "most_cited_work": obj.get("most_cited_work", "NULL"),
#                 "last_known_institution": obj.get("last_known_institution", "NULL"),
#                 "summary_stats": obj.get("summary_stats", "NULL"),
#                 "works_api_url": obj.get("works_api_url", "NULL"),
#             }
#             yield a_data


def process_json_file(efile_path):
    with open(efile_path, 'r') as file:
        for line in file:
            if line.strip():  # 确保行非空
                obj = json.loads(line)
                a_data = {
                    "aid": obj.get("aid"),
                    "display_name": obj.get("display_name", "NULL"),
                    "cited_by_count": obj.get("cited_by_count", "NULL"),
                    "counts_by_year": obj.get("counts_by_year", "NULL"),
                    "works_count": obj.get("works_count", "NULL"),
                    "most_cited_work": obj.get("most_cited_work", "NULL"),
                    "last_known_institution": obj.get("last_known_institution", "NULL"),
                    "summary_stats": obj.get("summary_stats", "NULL"),
                    "works_api_url": obj.get("works_api_url", "NULL"),
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


# 按装订区域中的绿色按钮以运行脚本。
if __name__ == '__main__':
    current_dir = os.getcwd()
    #test_au_dir = "E:\\openalex-snapshot\\data\\authors"
    test_au_dir = "authors"

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

                # 处理JSON数据
                generator = process_json_file(extract_file_path)
                json_output_file = extract_file_path.replace("\\","_")
                json_output_file = json_output_file + ".json"
                print(json_output_file)
                write_json_data(generator, json_output_file)

                # 上传至服务器
                #remote_file_path = "/home/sa/Data-Script/authors/" + os.path.basename(json_output_file)
                #upload_file(json_output_file, remote_file_path, "116.63.49.180", 22, "sa", "@buaa-sa-13")

                # 清理本地文件
                os.remove(extract_file_path)
                os.remove(json_output_file)

    # for root, dirs, files in os.walk(test_au_dir):
    #     for file_name in files:
    #         directory_name = []
    #         # 检查文件是否是 .gz 压缩文件
    #         if file_name.endswith('.gz'):
    #             file_path = os.path.join(root, file_name)
    #             print(file_path)
    #             # 创建解压后的文件路径
    #             extract_file_path = os.path.splitext(file_path)[0]
    #
    #             # 解压缩文件
    #             with gzip.open(file_path, 'rb') as gz_file:
    #                 with open(extract_file_path, 'wb') as extracted_file:
    #                     extracted_file.write(gz_file.read())
    #
    #             for i in range(3):
    #                 # new_data.append(a_data)
    #                 directory_name.append(os.path.basename(file_path))
    #                 # print(directory_name)
    #                 # 获取目录部分的路径
    #                 file_path = os.path.dirname(file_path)
    #
    #         print(file_name)
    #         file_name = file_name[:-3]
    #         temp_name = file_name
    #
    #         file_path = os.path.join(root, file_name)
    #
    #         # file_list = os.listdir(file_name)
    #         # print(file_list)
    #         # file_path = os.path.join(root, file_name)
    #         # print(file_path)
    #         with open(file_path, 'r') as file:
    #             content = file.read()
    #
    #         num = 0
    #         content = '[' + content + ']'
    #         for char in content:
    #             if char == "\n":
    #                 num += 1
    #
    #         content = content.replace("}\n{", "},{")
    #         data = json.loads(content)
    #         # 转换数据格式
    #         new_data = []
    #         for index in range(num):
    #             a_data = {
    #                 "aid": data[index].get("id"),
    #                 "display_name": data[index].get("display_name", "NULL"),
    #                 "cited_by_count": data[index].get("cited_by_count", "NULL"),
    #                 "counts_by_year": data[index].get("counts_by_year", "NULL"),
    #                 "works_count": data[index].get("works_count", "NULL"),
    #                 "most_cited_work": data[index].get("most_cited_work", "NULL"),
    #                 "last_known_institution": data[index].get("last_known_institution", "NULL"),
    #                 "summary_stats": data[index].get("summary_stats", "NULL"),
    #                 "works_api_url": data[index].get("works_api_url", "NULL"),
    #                 # "display_name": data[index]["display_name"]
    #             }
    #             new_data.append(a_data)
    #
    #         file_name = file_name + ".json"
    #         # 输出到 data.json 文件
    #         file_name = directory_name[2] + "_" + directory_name[1] + "_" + file_name
    #         with open(file_name, 'w') as file:
    #             file.write('[')
    #             # json.dump(new_data, file, indent=4)
    #             for index in range(num - 1):
    #                 json.dump(new_data[index], file, indent=4)
    #                 file.write(',')
    #             json.dump(new_data[num - 1], file, indent=4)
    #             file.write(']')
    #
    #         # 此时在当前目录下有相应的json文件,需上传至服务器
    #         local_file_path = os.path.join(current_dir, file_name)
    #         remote_file_path = "/home/sa/Data-Script/authors/" + file_name
    #         hostname = "116.63.49.180"
    #         port = 22  # 默认SSH端口为22
    #         username = "sa"
    #         password = "@buaa-sa-13"
    #
    #         upload_file(local_file_path, remote_file_path, hostname, port, username, password)
    #
    #         file_path = os.path.join(directory_name[2], directory_name[1], temp_name)
    #
    #         if os.path.exists(file_path):
    #             os.remove(file_path)
    #         if os.path.exists(file_name):
    #             os.remove(file_name)
