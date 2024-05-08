from typing import Callable, Dict, List
import time
import uuid
import json
from multiprocessing import Process, Queue
import traceback

from pathlib import Path

from fleet.utils.file_utils import safe_load_json


def run_job(job_func, job_input, info, output_queue):
    try:
        result = job_func(job_input, info)
        output_queue.put(result)
    except Exception as e:
        error_message = traceback.format_exc()
        print(error_message)
        output_queue.put({"error": error_message, "status": "crashed"})


class Worker:
    def __init__(self, args, job_func: Callable, info: Dict = {}):
        self.base_dir = Path(args.base_dir)
        self.timeout = args.timeout
        self.wait_manager = args.wait_manager
        self.max_job = args.max_job
        self.max_work_time = args.max_work_time
        unique_id = str(uuid.uuid4())
        self.node_id = f"{args.node_id}_{unique_id}" if args.node_id else unique_id
        self.nodes_dir = self.base_dir / 'nodes'
        self.status_dir = self.base_dir / 'status'
        self.heart_dir = self.base_dir / 'heart'
        self.finished_file = self.base_dir / 'finished'
        self.available_dir = self.base_dir / 'available'
        self.available_file = self.available_dir / self.node_id

        self.node_status_path = self.nodes_dir / f'{self.node_id}.status'
        self.node_heart_status_path = self.heart_dir / f'{self.node_id}.heart'

        self.job_func = job_func
        self.info = info

        # self.unassigned_task_status = {}
        self.not_find_job_num = 0

        self.heartbeat_process = None  # 添加一个属性来保存心跳进程的引用

        if self.wait_manager:
            wait_time = 0
            missing_dirs = self.check_dirs()
            while len(missing_dirs) > 0:
                if wait_time % 30 == 0:
                    print(f"Waiting for manager to create dirs...")
                    for missing_dir in missing_dirs:
                        print(f"Missing dir: {missing_dir}")
                time.sleep(1)
                wait_time += 1
                missing_dirs = self.check_dirs()

        else:
            missing_dirs = self.check_dirs()
            assert len(missing_dirs) == 0, f"Missing dirs: {missing_dirs}"

        self.finished_job_num = 0
        self.worker_start_time = time.time()

    def check_dirs(self) -> List[str]:
        missing_dirs = []
        for dir in [self.nodes_dir, self.status_dir, self.heart_dir, self.available_dir]:
            if not dir.exists():
                missing_dirs.append(str(dir))
        return missing_dirs

    def create_available_file(self):
        if not self.available_file.exists():
            self.available_file.touch()

    def heartbeat_daemon(self):
        """这个函数将作为独立的进程运行，负责发送心跳信号。"""
        try:
            while True:
                self.send_heartbeat()
                time.sleep(10)  # 设定心跳频率，例如每10秒发送一次心跳
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
        self.start_heartbeat()  # 在任务开始时启动心跳进程

        # for task_status_file in self.status_dir.iterdir():
        # status_info = safe_load_json(task_status_file)
        # if status_info and status_info["status"] == "unassigned":
        #     self.unassigned_task_status[task_status_file.stem] = status_info
        # print("Read task status done")
        node_info = {
            "status": "idle"
        }
        # self.node_status_path.write_text('idle')
        self.node_status_path.write_text(json.dumps(node_info))
        self.create_available_file()
        print(f"Node {self.node_id} registered")

    def process_job(self):
        find_job = False
        node_info = safe_load_json(self.node_status_path)
        if node_info and node_info['status'] == 'busy':
            task_status_path = node_info['task_status_path']
            status_info = safe_load_json(Path(task_status_path))
            task_status_file = Path(node_info["task_status_path"])

            job_input = status_info.get('input')
            print(f"Processing task: {job_input}")
            find_job = True

            if self.timeout is None:
                result = self.job_func(job_input, self.info)
            else:
                output_queue = Queue()
                job_process = Process(target=run_job, args=(self.job_func, job_input, self.info, output_queue))
                job_process.start()
                job_process.join(timeout=self.timeout)
                if job_process.is_alive():
                    job_process.terminate()
                    job_process.join()
                    result = {"error": "job timeout", "status": "crashed"}
                else:
                    result = output_queue.get()

            print(f"Task {job_input} Done!")
            self.finished_job_num += 1
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
            worker_status = self.check_worker_status()
            if worker_status == "running":
                self.create_available_file()
            return find_job
        return find_job

    def check_and_process_tasks(self):
        find_job = self.process_job()

        if not find_job:
            if self.not_find_job_num % 100 == 20:
                print("No task assigned...")

            if self.not_find_job_num < 20:
                time.sleep(0.1)
            else:
                time.sleep(0.5)

            self.not_find_job_num += 1
        else:
            self.not_find_job_num = 0

    def check_worker_status(self)->str:
        if self.max_job is not None and self.finished_job_num >= self.max_job:
            return "max_job_reached"
        if self.max_work_time is not None and time.time() - self.worker_start_time > self.max_work_time:
            return "max_work_time_reached"
        if self.finished_file.exists():
            return "finished_file_exists"
        if not self.check_heart():
            return "heart_dead"
        return "running"

    def run(self):
        self.register_node()

        try:
            while True:
                self.check_and_process_tasks()
                worker_status = self.check_worker_status()
                if worker_status == "finished_file_exists":
                    node_info = safe_load_json(self.node_status_path)
                    if node_info and node_info['status'] == 'busy':
                        continue

                    print("Finished file exists, exit!")
                    break
                if worker_status != "running":
                    print(f"Worker finish reason: {worker_status}")
                    break

        except Exception as e:
            # traceback.print_exc()  # 打印异常信息和堆栈跟踪
            error_message = traceback.format_exc()
            print(error_message)
        finally:
            self.stop_heartbeat()  # 在任何结束时确保心跳进程被终止
