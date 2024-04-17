from typing import Callable, Dict
import time
import uuid
import json
from copy import deepcopy
from multiprocessing import Process

from pathlib import Path

from fleet.utils.file_utils import safe_load_json

class Worker:
    def __init__(self, args, job_func: Callable, info: Dict = {}):
        self.base_dir = Path(args.base_dir)
        unique_id = str(uuid.uuid4())
        self.node_id = f"{args.node_id}_{unique_id}" if args.node_id else unique_id
        self.nodes_dir = self.base_dir / 'nodes'
        self.status_dir = self.base_dir / 'status'
        self.heart_dir = self.base_dir / 'heart'

        self.node_status_path = self.nodes_dir / f'{self.node_id}.status'
        self.node_heart_status_path = self.heart_dir / f'{self.node_id}.heart'

        self.job_func = job_func
        self.info = info

        self.unassigned_task_status = {}
        self.not_find_job_num = 0

        self.heartbeat_process = None  # 添加一个属性来保存心跳进程的引用

    def heartbeat_daemon(self):
        """这个函数将作为独立的进程运行，负责发送心跳信号。"""
        try:
            while True:
                self.send_heartbeat()
                time.sleep(1)  # 设定心跳频率，例如每1秒发送一次心跳
        except KeyboardInterrupt:
            pass  # 这里可以捕捉 KeyboardInterrupt 异常来优雅地处理进程终止

    def send_heartbeat(self, status: str = 'available'):
        current_time = int(time.time())
        while True:
            retry_time = 0
            try:
                retry_time += 1
                self.node_heart_status_path.write_text(json.dumps({"status": status, "last_heartbeat": current_time}))
                break
            except:
                print(f"Failed to write heart status file {self.node_heart_status_path}, tried {retry_time} times")
                time.sleep(1)
                if retry_time > 20:
                    print(f"Failed to write heart status file {self.node_heart_status_path}")
                    break

    def check_heart(self):
        heart_status = safe_load_json(self.node_heart_status_path)
        if heart_status is None or heart_status['status'] == 'dead':
            return False
        return True

    def start_heartbeat(self):
        """启动心跳进程。"""
        self.heartbeat_process = Process(target=self.heartbeat_daemon)
        self.heartbeat_process.start()

    def stop_heartbeat(self):
        """停止心跳进程。"""
        if self.heartbeat_process:
            self.heartbeat_process.terminate()
            self.heartbeat_process.join()
            self.send_dead()  # 在进程终止时发送死亡信号

    def send_dead(self):
        self.send_heartbeat(status='dead')

    def register_node(self):
        for task_status_file in self.status_dir.iterdir():
            status_info = safe_load_json(task_status_file)
            if status_info and status_info["status"] == "unassigned":
                self.unassigned_task_status[task_status_file.stem] = status_info
        print("Read task status done")
        node_info = {
            "status": "idle"
        }
        # self.node_status_path.write_text('idle')
        self.node_status_path.write_text(json.dumps(node_info))
        self.start_heartbeat()  # 在任务开始时启动心跳进程

        print(f"Node {self.node_id} registered")

    def check_and_process_tasks(self):
        find_job = False
        for task_name in list(self.unassigned_task_status.keys()):
            status_info = deepcopy(self.unassigned_task_status[task_name])
            task_status_file = Path(status_info["task_status_path"])

            status_info_in_file = safe_load_json(task_status_file)
            if status_info_in_file is None:
                continue

            if status_info_in_file['status'] != 'unassigned':
                del self.unassigned_task_status[task_name]
            if status_info_in_file.get('assigned_to') == self.node_id:
                # 标记节点为忙碌
                # self.node_status_path.write_text('busy')

                job_input = status_info.get('input')
                print(f"Processing task: {job_input}")
                find_job = True
                result = self.job_func(job_input, self.info)
                print(f"Task {job_input} Done!")
                if 'error' in result:
                    status_info['error'] = result['error']

                # 更新任务状态为完成
                status_info['status'] = result['status']

                task_status_file.write_text(json.dumps(status_info))

                # 标记节点为空闲
                node_info = {
                    "status": "idle"
                }
                self.node_status_path.write_text(json.dumps(node_info))
                break

        if not find_job:
            if self.not_find_job_num % 100 == 0:
                print("No task assigned...")
            time.sleep(0.5)
            self.not_find_job_num += 1
        else:
            self.not_find_job_num = 0

    def run(self):
        self.register_node()

        try:
            while True:
                self.check_and_process_tasks()
                if len(self.unassigned_task_status) == 0:
                    break
                if not self.check_heart():
                    break

        except Exception as e:
            print(f"Error: {e}")
        finally:
            self.stop_heartbeat()  # 在任何结束时确保心跳进程被终止
