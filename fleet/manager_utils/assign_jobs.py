import json
import time
from copy import deepcopy
import threading

from pathlib import Path


def loop_assignment(available_dir, nodes_dir, working_dir, unassigned_task_status, console):
    while len(unassigned_task_status) > 0:
        unassigned_task_status, working_task_status = process_assignment(available_dir, nodes_dir, working_dir,
                                                                         unassigned_task_status, console)
        if len(working_task_status) == 0:
            time.sleep(0.1)
    console.log("All tasks are assigned.")


def process_assignment(available_dir, nodes_dir, working_dir: Path, unassigned_task_status, console):
    working_task_status = {}
    if len(unassigned_task_status) == 0:
        return unassigned_task_status, working_task_status

    available_nodes = get_available_nodes(available_dir, nodes_dir)
    if available_nodes:
        unassigned_task_status, working_task_status = assign_job_to_node(available_nodes,
                                                                         unassigned_task_status, working_dir, console)

    return unassigned_task_status, working_task_status


def get_available_nodes(available_dir, nodes_dir):
    files = list(available_dir.glob('*'))
    available_nodes = []
    for file in files:
        file_name = file.name
        node_file = nodes_dir / f"{file_name}.status"
        available_nodes.append((file_name, node_file, file))
    return available_nodes


def do_assign_job(process_input):
    chosen_node, node_file, available_file, status_info, working_dir, job_key, console = process_input
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
    if available_file.exists():
        available_file.unlink()
    console.log(f"Assign task {job_key} to node {chosen_node}")

    working_file = working_dir / job_key
    if not working_file.exists():
        # console.log(f"Writing {str(working_file)}")
        working_file.write_text(json.dumps(status_info))


def assign_job_to_node(available_nodes, unassigned_task_status, working_dir, console):
    working_task_status = {}

    process_inputs = []

    for job_key in list(unassigned_task_status.keys()):
        status_info = deepcopy(unassigned_task_status[job_key])
        assert status_info['status'] == 'unassigned'
        if available_nodes:
            chosen_node, node_file, available_file = available_nodes.pop()
            process_inputs.append((chosen_node, node_file, available_file, status_info, working_dir, job_key, console))
            # 移除未完成任务
            del unassigned_task_status[job_key]
            working_task_status[job_key] = status_info
        else:
            break

    # for process_input in process_inputs:
    #     do_assign_job(process_input)

    threads = []
    for process_input in process_inputs:
        thread = threading.Thread(target=do_assign_job, args=(process_input,))
        threads.append(thread)
        thread.start()  # 启动线程

    # 等待所有线程完成
    for thread in threads:
        thread.join()

    if len(process_inputs) > 0:
        console.log(f"Assigned jobs to {len(process_inputs)} nodes.")

    return unassigned_task_status, working_task_status
