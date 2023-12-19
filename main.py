# 这是一个示例 Python 脚本。
import pandas as pd
import os
import json
import gzip

# 按 Shift+F10 执行或将其替换为您的代码。
# 按 双击 Shift 在所有地方搜索类、文件、工具窗口、操作和设置。


def print_hi(name):
    # 在下面的代码行中使用断点来调试脚本。
    print(f'Hi, {name}')  # 按 Ctrl+F8 切换断点。


# 按装订区域中的绿色按钮以运行脚本。
if __name__ == '__main__':
    current_dir = os.getcwd()
    test_au_dir = os.path.join(current_dir, "test_au")

    for root, dirs, files in os.walk(test_au_dir):
        for file_name in files:
            # 检查文件是否是 .gz 压缩文件
            if file_name.endswith('.gz'):
                file_path = os.path.join(root, file_name)

                # 创建解压后的文件路径
                extract_file_path = os.path.splitext(file_path)[0]

                # 解压缩文件
                with gzip.open(file_path, 'rb') as gz_file:
                    with open(extract_file_path, 'wb') as extracted_file:
                        extracted_file.write(gz_file.read())

            print(file_name)
            file_name = file_name[:-3]
            # file_path = os.path.join(root, file_name)
            # print(file_path)
            with open(file_name, 'r') as file:
                content = file.read()

            num = 0
            content = '[' + content + ']'
            for char in content:
                if char == "\n":
                    num += 1

            content = content.replace("}\n{", "},{")
            data = json.loads(content)
            # 转换数据格式
            new_data = []
            for index in range(num):
                a_data = {
                    "id": data[index]["id"],
                    "display_name": data[index]["display_name"],
                    "cited_by_count": data[index]["cited_by_count"],
                    "counts_by_year": data[index]["counts_by_year"],
                    "works_count": data[index]["works_count"],
                    "most_cited_work": data[index]["most_cited_work"],
                    "last_known_institution": data[index]["last_known_institution"],
                    "summary_stats": data[index]["summary_stats"],
                    "works_api_url": data[index]["works_api_url"],
                    # "display_name": data[index]["display_name"]
                }
                new_data.append(a_data)

            file_name = file_name+".json"
            # 输出到 data.json 文件
            with open(file_name, 'w') as file:
                file.write('[')
                # json.dump(new_data, file, indent=4)
                for index in range(num - 1):
                    json.dump(new_data[index], file, indent=4)
                    file.write(',')
                json.dump(new_data[num - 1], file, indent=4)
                file.write(']')

            # 此时在当前目录下有相应的json文件,需上传至服务器


    #以下为单文件处理
    # with open('part_000', 'r') as file:
    # content = file.read()
    #
    # num = 0
    # content = '['+content+']'
    # for char in content:
    #     if char == "\n":
    #         num+=1
    #
    #
    # content = content.replace("}\n{", "},{")
    # data = json.loads(content)
    # # 转换数据格式
    # new_data = []
    # for index in range(num):
    #     a_data = {
    #         "id": data[index]["id"],
    #         "display_name": data[index]["display_name"],
    #         "cited_by_count": data[index]["cited_by_count"],
    #         "counts_by_year": data[index]["counts_by_year"],
    #         "works_count": data[index]["works_count"],
    #         "most_cited_work": data[index]["most_cited_work"],
    #         "last_known_institution": data[index]["last_known_institution"],
    #         "summary_stats": data[index]["summary_stats"],
    #         "works_api_url":data[index]["works_api_url"],
    #         # "display_name": data[index]["display_name"]
    #     }
    #     new_data.append(a_data)
    #
    # # 输出到 data.json 文件
    # with open('data2.json', 'w') as file:
    #     file.write('[')
    #     # json.dump(new_data, file, indent=4)
    #     for index in range(num-1):
    #         json.dump(new_data[index], file, indent=4)
    #         file.write(',')
    #     json.dump(new_data[num-1], file, indent=4)
    #     file.write(']')
    print_hi('PyCharm')

# 访问 https://www.jetbrains.com/help/pycharm/ 获取 PyCharm 帮助
