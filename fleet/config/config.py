from typing import Optional, List, Union
import argparse


def get_args(input: Optional[Union[str, List[str]]] = None):
    parser = argparse.ArgumentParser(description="Runner")
    parser.add_argument("--base_dir", default=None, type=str,
                        help="directory to store the status of the nodes and tasks")
    parser.add_argument("--node_id", default=None, type=str, help="node id for the worker")
    if input is not None:
        if isinstance(input, str):
            input = [element for element in input.split(" ") if element]
        args = parser.parse_args(input)
    else:
        args = parser.parse_args()
    return args
