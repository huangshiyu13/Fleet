import time

from run_manager import base_dir

from fleet.config.config import get_args
from fleet.worker import Worker


def add_one(x, info):
    # time.sleep(2)
    return {"status": "success", "result": x + 1}


def main():
    wait_manager = True
    max_job = 2
    max_work_time = 10
    args_input = f"--base_dir {base_dir}"
    if wait_manager:
        args_input += " --wait_manager"
    if max_job:
        args_input += f" --max_job {max_job}"
    if max_work_time:
        args_input += f" --max_work_time {max_work_time}"
    args = get_args(args_input)
    worker = Worker(args=args, job_func=add_one)
    worker.run()


if __name__ == "__main__":
    main()
