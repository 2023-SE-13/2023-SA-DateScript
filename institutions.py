# 这是一个示例 Python 脚本。
import pandas as pd
import os
import json
import gzip

# 按装订区域中的绿色按钮以运行脚本。
if __name__ == '__main__':
    current_dir = os.getcwd()
    test_au_dir = os.path.join(current_dir, "institutions")

    for root, dirs, files in os.walk(test_au_dir):
        for file_name in files:
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

                for i in range(3):
                    # new_data.append(a_data)
                    directory_name.append(os.path.basename(file_path))
                    # print(directory_name)
                    # 获取目录部分的路径
                    file_path = os.path.dirname(file_path)

            print(file_name)
            file_name = file_name[:-3]

            file_path = os.path.join(root, file_name)

            # file_list = os.listdir(file_name)
            # print(file_list)
            # file_path = os.path.join(root, file_name)
            # print(file_path)
            with open(file_path, 'r') as file:
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
                        "iid": data[index].get("id", "NULL"),
                        "associated_institutions": data[index].get("associated_institutions", "NULL"),
                        "cited_by_count": data[index].get("cited_by_count", "NULL"),
                        "country_code": data[index].get("country_code", "NULL"),
                        "counts_by_year": data[index].get("counts_by_year", "NULL"),
                        "display_name": data[index].get("display_name", "NULL"),
                        "display_name_acronyms": data[index].get("display_name_acronyms", "NULL"),
                        "homepage_url": data[index].get("homepage_url", "NULL"),
                        "summary_stats": data[index].get("summary_stats", "NULL"),
                        "type": data[index].get("type", "NULL"),
                        "works_api_url": data[index].get("works_api_url", "NULL"),
                        "works_count": data[index].get("works_count", "NULL"),
                        "image_thumbnail_url": data[index].get("image_thumbnail_url", "NULL"),
                    # "display_name": data[index]["display_name"]
                }
                new_data.append(a_data)

            file_name = file_name+".json"
            # 输出到 data.json 文件
            file_name = directory_name[2]+"_"+directory_name[1]+"_"+file_name
            with open(file_name, 'w') as file:
                file.write('[')
                # json.dump(new_data, file, indent=4)
                for index in range(num - 1):
                    json.dump(new_data[index], file, indent=4)
                    file.write(',')
                json.dump(new_data[num - 1], file, indent=4)
                file.write(']')

            # 此时在当前目录下有相应的json文件,需上传至服务器

