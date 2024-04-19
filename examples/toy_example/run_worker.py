import time

from run_manager import base_dir

from fleet.config.config import get_args
from fleet.worker import Worker


def add_one(x, info):
    # time.sleep(5)
    return {"status": "success", "result": x + 1}


def main():
    args = get_args(f"--base_dir {base_dir}")
    worker = Worker(args=args, job_func=add_one)
    worker.run()


if __name__ == "__main__":
    main()
