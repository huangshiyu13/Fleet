from fleet.config.config import get_args
from fleet.manager import Manager

base_dir = "./share_dir"


def main():
    job_list = [1, 2, 3, 4]
    args = get_args(f"--base_dir {base_dir}")
    manager = Manager(args=args, job_list=job_list)
    manager.run()


if __name__ == "__main__":
    main()
