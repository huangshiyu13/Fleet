from typing import List, Any, Dict
import time
import json
from multiprocessing import Process
from datetime import datetime

from pathlib import Path
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn

from fleet.utils.file_utils import safe_load_json
from fleet.utils.time_tracker import TimeTracker
from fleet.manager_utils.assign_jobs import loop_assignment


class Manager:
    def __init__(self, args, job_list: List[Any], info: Dict = {}):
        base_dir = args.base_dir
        self.base_dir = Path(base_dir)
        self.console = Console()
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.nodes_dir = self.base_dir / 'nodes'

        self.status_dir = self.base_dir / 'status'
        self.working_dir = self.base_dir / 'working'

        self.heart_dir = self.base_dir / 'heart'

        # to store the available nodes
        self.available_dir = self.base_dir / 'available'

        self.finished_file = self.base_dir / 'finished'

        self.nodes_dir.mkdir(parents=True, exist_ok=True)
        print(f"nodes_dir: {self.nodes_dir}")
        self.heart_dir.mkdir(parents=True, exist_ok=True)
        print(f"heart_dir: {self.heart_dir}")
        self.status_dir.mkdir(parents=True, exist_ok=True)
        print(f"status_dir: {self.status_dir}")
        self.available_dir.mkdir(parents=True, exist_ok=True)
        print(f"available_dir: {self.available_dir}")
        self.working_dir.mkdir(parents=True, exist_ok=True)

        self.job_list = job_list
        self.total_jobs = len(job_list)
        self.info = info
        # self.working_task_status = {}
        self.working_num = 0
        self.unassigned_task_status = {}

        self.no_available_nodes_num = 0

        self.success_num = 0
        self.crashed_num = 0
        self.failed_num = 0

        self.dead_nodes = {}
        self.available_nodes = {}

        self.time_tracker = TimeTracker(total_tasks=self.total_jobs)

        self.first_assigned = True

        self.job_assign_process = None

        self.previous_log_time = None
        # self.new_finished_num = 0

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
                    # self.working_task_status[task_name] = status_info
                    # self.working_task_status[task_name]["task_status_path"] = str(task_status_path)
                    working_file = self.working_dir / task_name
                    if not working_file.exists():
                        status_info["task_status_path"] = str(task_status_path)
                        working_file.write_text(json.dumps(status_info))

        if self.finished_num == 0:
            success_rate = 0
        else:
            success_rate = self.success_num / self.finished_num * 100

        self.progress.update(self.task_id,
                             description=f"Success Rate: {success_rate:.2f}% Finished: {self.finished_num}/{self.total_jobs}")

        if self.finished_num > 0:
            self.first_assigned = False

    def process_dead_nodes(self, dead_nodes):
        # check if the task is assigned to a dead node
        for node in dead_nodes:
            node_file = self.nodes_dir / f"{node}.status"
            node_info = safe_load_json(node_file)
            if node_info and node_info["status"] == 'busy':
                task_status_path = node_info["task_status_path"]

                status_info = safe_load_json(Path(task_status_path))

                if status_info and status_info['status'] == 'assigned':
                    status_info['status'] = 'crashed'
                    Path(task_status_path).write_text(json.dumps(status_info))
                    node_info['status'] = 'dead'
                    node_file.write_text(json.dumps(node_info))

    def log_status(self):
        current_time = time.time()
        if self.previous_log_time is None or current_time - self.previous_log_time > 1 or self.finished_num == self.total_jobs:
            self.previous_log_time = current_time
        else:
            return

        if self.finished_num == 0:
            success_rate = 0
        else:
            success_rate = self.success_num / self.finished_num * 100
        time_summary = self.time_tracker.summary

        self.progress.update(self.task_id,
                             description=f"Success Rate: {success_rate:.2f}% Finished/Working: {self.finished_num}/{self.working_num}/{self.total_jobs} Nodes(Good/Dead): {len(self.available_nodes)}/{len(self.dead_nodes)} {time_summary}")

    def check_task_status_and_assign(self):
        self.monitor_heartbeats()
        self.check_working_tasks()
        self.log_status()

    def check_working_tasks(self):
        self.working_num = 0
        # self.new_finished_num = 0
        # Remove finished tasks in working_task_status
        working_files = list(self.working_dir.glob("*"))

        # for job_key in list(self.working_task_status.keys()):
        for working_file in working_files:
            # status_info = self.working_task_status[job_key]
            status_info = safe_load_json(working_file)
            task_status_file = Path(status_info["task_status_path"])

            assert task_status_file.exists(), f"task_status_file {task_status_file} not exists"

            status_info_in_file = safe_load_json(task_status_file)
            if status_info_in_file is None:
                continue

            assert status_info_in_file['status'] in ["assigned", "success", "crashed", "failed"]

            if status_info_in_file['status'] == "assigned":
                self.working_num += 1
            else:
                if status_info_in_file['status'] == 'success':
                    self.success_num += 1
                elif status_info_in_file['status'] == 'crashed':
                    self.crashed_num += 1
                elif status_info_in_file['status'] == 'failed':
                    self.failed_num += 1

                # del self.working_task_status[job_key]
                working_file.unlink()

                self.finished_num += 1
                # self.new_finished_num += 1
                # self.console.log(f"{self.finished_num}/{self.new_finished_num}")

                self.time_tracker.update()
                self.progress.update(self.task_id, advance=1)

    def loop_assignment(self):
        loop_assignment(self.available_dir, self.nodes_dir, self.working_dir, self.unassigned_task_status, self.console)

    def start_job_assignment(self):
        self.job_assign_process = Process(target=self.loop_assignment)
        self.job_assign_process.start()

    def stop_job_assignment(self):
        if self.job_assign_process:
            self.job_assign_process.terminate()
            self.job_assign_process.join()

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

            self.start_job_assignment()

            try:
                while True:
                    if self.finished_num == self.total_jobs or self.working_num + self.finished_num == self.total_jobs:
                        if not self.finished_file.exists():
                            self.finished_file.touch()

                    if self.finished_num == self.total_jobs:
                        break

                    self.check_task_status_and_assign()

            finally:
                self.stop_job_assignment()

    def monitor_heartbeats(self, heartbeat_timeout: int = 120):
        self.available_nodes = {}
        new_dead_nodes = {}

        current_time = int(time.time())
        for node_file in self.heart_dir.iterdir():
            if node_file.stem in self.dead_nodes:
                continue

            node_info = safe_load_json(node_file)

            if node_info and node_info['status'] == "available" and current_time - node_info.get("last_heartbeat",
                                                                                                 0) <= heartbeat_timeout:
                self.available_nodes[node_file.stem] = node_info

            else:
                if node_info is None:
                    dead_reason = f"can not load node info from {str(node_file)}"
                else:
                    if node_info['status'] == 'dead':
                        dead_reason = f"worker sends dead"
                    else:
                        dead_reason = f"no heartbeat, last heartbeat: {datetime.fromtimestamp(node_info.get('last_heartbeat', 0)).strftime('%Y-%m-%d %H:%M:%S')}"
                node_info['status'] = 'dead'
                node_info['dead_reason'] = dead_reason

                node_file.write_text(json.dumps(node_info))
                self.dead_nodes[node_file.stem] = node_info
                new_dead_nodes[node_file.stem] = node_info
        self.process_dead_nodes(new_dead_nodes)
