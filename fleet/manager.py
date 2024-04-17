from typing import List, Any, Dict
import time
import json

from pathlib import Path
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn

from fleet.utils.file_utils import safe_load_json
from fleet.utils.time_tracker import TimeTracker


class Manager:
    def __init__(self, args, job_list: List[Any], info: Dict = {}):
        base_dir = args.base_dir
        self.base_dir = Path(base_dir)
        self.console = Console()
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.nodes_dir = self.base_dir / 'nodes'

        self.status_dir = self.base_dir / 'status'
        self.heart_dir = self.base_dir / 'heart'

        self.nodes_dir.mkdir(parents=True, exist_ok=True)
        print(f"nodes_dir: {self.nodes_dir}")
        self.heart_dir.mkdir(parents=True, exist_ok=True)
        print(f"heart_dir: {self.heart_dir}")
        self.status_dir.mkdir(parents=True, exist_ok=True)
        print(f"status_dir: {self.status_dir}")

        self.job_list = job_list
        self.total_jobs = len(job_list)
        self.info = info
        self.working_task_status = {}
        self.unassigned_task_status = {}

        self.no_available_nodes_num = 0

        self.success_num = 0
        self.crashed_num = 0
        self.failed_num = 0

        self.dead_nodes = {}
        self.available_nodes = {}

        self.time_tracker = TimeTracker(total_tasks=self.total_jobs)

    def initialize_tasks(self):
        self.finished_num = 0
        for idx, job_input in enumerate(self.job_list):
            task_name = f'task{idx + 1}'
            task_status_path = self.status_dir / f'{task_name}.status'
            if not task_status_path.exists():
                status_info = {'status': 'unassigned', 'input': job_input, "task_status_path": str(task_status_path)}
                task_status_path.write_text(json.dumps(status_info))
            else:
                status_info = safe_load_json(task_status_path)

            if status_info is None:
                continue

            if status_info['status'] in ["success", "crashed", "failed"]:
                self.finished_num += 1
                self.time_tracker.update()
                if status_info['status'] == 'success':
                    self.success_num += 1
                elif status_info['status'] == 'crashed':
                    self.crashed_num += 1
                elif status_info['status'] == 'failed':
                    self.failed_num += 1

                self.progress.update(self.task_id, advance=1)

            else:
                if status_info['status'] == 'unassigned':
                    self.unassigned_task_status[task_name] = status_info
                    self.unassigned_task_status[task_name]["task_status_path"] = str(task_status_path)
                else:
                    assert status_info['status'] == 'assigned'
                    self.working_task_status[task_name] = status_info
                    self.working_task_status[task_name]["task_status_path"] = str(task_status_path)

        if self.finished_num == 0:
            success_rate = 0
        else:
            success_rate = self.success_num / self.finished_num * 100
        time_summary = self.time_tracker.summary
        self.progress.update(self.task_id,
                             description=f"Success Rate: {success_rate:.2f}% {self.finished_num}/{self.total_jobs} {time_summary}")

    def check_task_status_and_assign(self):
        self.monitor_heartbeats()

        available_nodes = []
        for node in self.available_nodes:
            node_file = self.nodes_dir / f"{node}.status"
            node_info = safe_load_json(node_file)
            if node_info and node_info["status"] == 'idle':
                available_nodes.append((node, node_file))

        for node in self.dead_nodes:
            node_file = self.nodes_dir / f"{node}.status"
            node_info = safe_load_json(node_file)
            if node_info and node_info["status"] == 'busy':
                task_status_path = node_info["task_status_path"]

                status_info = safe_load_json(Path(task_status_path))

                if status_info and status_info['status'] == 'assigned':
                    status_info['status'] = 'crashed'
                    Path(task_status_path).write_text(json.dumps(status_info))
                    node_info['status'] = 'idle'
                    node_file.write_text(json.dumps(node_info))

        if not available_nodes:
            if self.no_available_nodes_num % 100 == 10:
                self.console.log("No available nodes, sleep 1 seconds...")

            if self.no_available_nodes_num % 100 < 10:
                time.sleep(1)
            else:
                time.sleep(10)

            self.no_available_nodes_num += 1

        else:
            self.no_available_nodes_num = 0
            self.assign_task_to_node(available_nodes)

        self.check_working_tasks()

        # log status
        if self.finished_num == 0:
            success_rate = 0
        else:
            success_rate = self.success_num / self.finished_num * 100

        time_summary = self.time_tracker.summary
        self.progress.update(self.task_id,
                             description=f"Success Rate: {success_rate:.2f}% {self.finished_num}/{self.total_jobs} Nodes(Good/Dead): {len(self.available_nodes)}/{len(self.dead_nodes)} {time_summary}")

    def assign_task_to_node(self, available_nodes):
        assign_new_node_num = 0
        for job_key in list(self.unassigned_task_status.keys()):
            status_info = self.unassigned_task_status[job_key]
            assert status_info['status'] == 'unassigned'
            if available_nodes:
                # self.console.log("available_nodes", available_nodes)
                chosen_node, node_file = available_nodes.pop()
                status_info['assigned_to'] = chosen_node
                status_info['status'] = 'assigned'
                task_status_file = Path(status_info["task_status_path"])
                task_status_file.write_text(json.dumps(status_info))

                node_info = {
                    "status": "busy",
                    "task": job_key,
                    "task_status_path": status_info["task_status_path"]
                }
                node_file.write_text(json.dumps(node_info))

                self.console.log(f"Assign task {job_key} to node {chosen_node}")
                assign_new_node_num += 1
                # 移除未完成任务
                del self.unassigned_task_status[job_key]
                # 添加到工作任务
                self.working_task_status[job_key] = status_info
            else:
                break
        if assign_new_node_num > 0:
            self.console.log(f"Assigned jobs to {assign_new_node_num} nodes.")
            # time.sleep(10)

    def check_working_tasks(self):
        for job_key in list(self.working_task_status.keys()):
            status_info = self.working_task_status[job_key]
            task_status_file = Path(status_info["task_status_path"])

            assert task_status_file.exists(), f"task_status_file {task_status_file} not exists"

            status_info_in_file = safe_load_json(task_status_file)
            if status_info_in_file is None:
                continue

            assert status_info_in_file['status'] in ["assigned", "success", "crashed", "failed"]

            if status_info_in_file['status'] == "assigned":
                continue
            else:
                if status_info_in_file['status'] == 'success':
                    self.success_num += 1
                elif status_info_in_file['status'] == 'crashed':
                    self.crashed_num += 1
                elif status_info_in_file['status'] == 'failed':
                    self.failed_num += 1

                del self.working_task_status[job_key]
                self.finished_num += 1
                self.time_tracker.update()
                self.progress.update(self.task_id, advance=1)

    def run(self):

        with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("{task.percentage:>3.0f}%"),
                console=self.console
        ) as progress:
            self.task_id = progress.add_task("Processing Jobs", total=self.total_jobs)
            self.progress = progress
            # self.pbar = tqdm(total=len(self.job_list), desc="Processing Jobs")
            self.initialize_tasks()

            while True:
                self.check_task_status_and_assign()
                # time.sleep(10)  # 每10秒检查一次
                if self.finished_num == self.total_jobs:
                    break

            # self.pbar.close()

    def monitor_heartbeats(self, heartbeat_timeout: int = 120):
        self.available_nodes = {}
        self.dead_nodes = {}

        current_time = int(time.time())
        for node_file in self.heart_dir.iterdir():
            node_info = safe_load_json(node_file)
            if node_info and node_info['status'] == "available" and current_time - node_info.get("last_heartbeat",
                                                                                                 0) <= heartbeat_timeout:
                self.available_nodes[node_file.stem] = node_info
            else:
                node_info['status'] = 'dead'
                node_file.write_text(json.dumps(node_info))
                self.dead_nodes[node_file.stem] = node_info
