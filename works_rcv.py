import os
import time
import json
import gzip
import threading
from elasticsearch import Elasticsearch, helpers
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Elasticsearch 配置
ES_HOST = "localhost"
ES_PORT = 9200
ES_USERNAME = "elastic"  # 替换为您的 Elasticsearch 用户名
ES_PASSWORD = "yXC0ZTAbjmhmyLHb7fBv"  # 替换为您的 Elasticsearch 密码
INDEX_NAME = "works"         # 根据您的需求更改

# 初始化 Elasticsearch 客户端
es = Elasticsearch(
    [{
        'scheme': 'http',
        'host': ES_HOST, 
        'port': ES_PORT
    }],
    basic_auth=(ES_USERNAME, ES_PASSWORD)
)


def decompress_gz(gz_path, output_path):
    with gzip.open(gz_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            f_out.write(f_in.read())


# 准备批量上传的函数
def bulk_index(file_path):
    temp_val = 0
    try:
        with open(file_path, 'r') as file:
            actions = []
            count = 0

            for line in file:
                json_data = json.loads(line)  # 解析每一行为 JSON
                action = {
                    "_index": INDEX_NAME,
                    "_source": json_data
                }
                actions.append(action)
                count += 1

                # 每读取100个数据就进行一次批量上传
                if count % 100 == 0:
                    helpers.bulk(es, actions)
                    # print("11111")
                    temp_val = temp_val + 1
                    actions = []  # 清空列表以便下一批数据

            # 处理剩余的数据（如果有）
            if actions:
                helpers.bulk(es, actions)

        return True
    except Exception as e:
        print(f"Error indexing file {file_path} {temp_val*100}: {e}")
        return False


# Elasticsearch 配置和初始化，以及 bulk_index 函数保持不变

class Watcher:
    DIRECTORY_TO_WATCH = "./works"

    def __init__(self):
        self.observer = Observer()

    def run(self):
        event_handler = Handler()
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except:
            self.observer.stop()
            print("Observer stopped")

        self.observer.join()

class Handler(FileSystemEventHandler):
    @staticmethod
    def handle_file(file_path, is_gzipped, done_file_path):
        if is_gzipped:
            json_file_path = file_path[:-3]  # 移除 ".gz" 后缀以获得JSON文件路径
            print(f"Begin to decompress file: {json_file_path}")
            decompress_gz(file_path, json_file_path)
            print(f"Decompress file done: {json_file_path}")
        else:
            json_file_path = file_path

        print(f"Begin to index JSON file: {json_file_path}")
        if bulk_index(json_file_path):
            os.remove(json_file_path)
            print(f"Finished indexing and deleted JSON file: {json_file_path}")
        else:
            print(f"Failed to index JSON file: {json_file_path}")
        if is_gzipped:
            # 删除原始的gzip文件
            os.remove(file_path)
        os.remove(done_file_path)


    @staticmethod
    def on_created(event):
        if event.is_directory:
            return None

        if event.event_type == 'created' and event.src_path.endswith('.done'):
            json_file_path = event.src_path[:-5]
            print(f"Detected .done file, processing: {json_file_path}")
            
            # 创建一个线程来处理JSON文件
            threading.Thread(target=Handler.handle_file, args=(json_file_path, False, event.src_path)).start()


        elif event.event_type == 'created' and event.src_path.endswith('.gzdone'):
            gzip_file_path = event.src_path[:-7]  # 移除 ".gzdone" 后缀

            print(f"Detected .gzdone file, processing: {gzip_file_path}")

            # 创建一个线程来处理gzip文件
            threading.Thread(target=Handler.handle_file, args=(gzip_file_path, True, event.src_path)).start()



if __name__ == '__main__':
    w = Watcher()
    w.run()
