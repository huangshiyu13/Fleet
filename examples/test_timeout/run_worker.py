import time
from fleet.worker import Worker
from fleet.config.config import get_args

from run_manager import base_dir


def add_one(x, info):
    time.sleep(5)
    return {"status": "success", "result": x + 1}


def main():
    timeout = 1
    args_input = f"--base_dir {base_dir}"
    if timeout is not None:
        args_input += f" --timeout {timeout}"
    args = get_args(args_input)
    worker = Worker(args=args, job_func=add_one)
    worker.run()


if __name__ == '__main__':
    main()
