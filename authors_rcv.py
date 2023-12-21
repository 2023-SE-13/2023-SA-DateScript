import os
import time
import json
from elasticsearch import Elasticsearch, helpers
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Elasticsearch 配置
ES_HOST = "localhost"
ES_PORT = 9200
ES_USERNAME = "elastic"  # 替换为您的 Elasticsearch 用户名
ES_PASSWORD = "yXC0ZTAbjmhmyLHb7fBv"  # 替换为您的 Elasticsearch 密码
INDEX_NAME = "authors"         # 根据您的需求更改
DOC_TYPE = "_doc"

# 初始化 Elasticsearch 客户端
es = Elasticsearch(
    [{
        'scheme': 'http',
        'host': ES_HOST, 
        'port': ES_PORT
    }],
    http_auth=(ES_USERNAME, ES_PASSWORD)
)

# 准备批量上传的函数
def bulk_index(file_path):
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            actions = [
                {
                    "_index": INDEX_NAME,
                    "_type": DOC_TYPE,
                    "_source": item
                }
                for item in data
            ]
            helpers.bulk(es, actions)
        return True
    except Exception as e:
        print(f"Error indexing file {file_path}: {e}")
        return False


# Elasticsearch 配置和初始化，以及 bulk_index 函数保持不变

class Watcher:
    DIRECTORY_TO_WATCH = "./authors"

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
    def on_created(event):
        if event.is_directory:
            return None

        if event.event_type == 'created' and event.src_path.endswith('.done'):
            # 获取JSON文件的路径
            json_file_path = event.src_path[:-5]
            print(f"Detected .done file, processing: {json_file_path}")

            if os.path.exists(json_file_path):
                if bulk_index(json_file_path):
                    os.remove(json_file_path)
                    print(f"Finished indexing and deleted file: {json_file_path}")
                else:
                    print(f"Failed to index file: {json_file_path}")

            # 删除.done文件
            os.remove(event.src_path)


if __name__ == '__main__':
    w = Watcher()
    w.run()
