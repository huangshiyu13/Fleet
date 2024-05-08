from typing import Optional, List, Union
import argparse


def get_args(input: Optional[Union[str, List[str]]] = None):
    parser = argparse.ArgumentParser(description="Runner")
    parser.add_argument("--base_dir", default=None, type=str,
                        help="directory to store the status of the nodes and jobs")
    parser.add_argument("--node_id", default=None, type=str, help="node id for the worker")
    parser.add_argument("--timeout",default=None, type=int, help="timeout for each job")
    parser.add_argument("--wait_manager", default=False, action="store_true", help="whether to wait manager")
    parser.add_argument("--max_job", default=None, type=int, help="max number of jobs to run for each worker")
    parser.add_argument("--max_work_time", default=None, type=int, help="max time (sec) to run for each worker")
    if input is not None:
        if isinstance(input, str):
            input = [element for element in input.split(" ") if element]
        args = parser.parse_args(input)
    else:
        args = parser.parse_args()
    return args
