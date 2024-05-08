# Fleet

Fleet is a generic distributed task distribution framework based on a distributed file system. Task distribution frameworks like Ray and Celery require network connections for communication, which makes them difficult to use in clusters with poor network conditions. Fleet is a distributed framework based on a shared file system, independent of any network communication, allowing for task distribution among nodes without any network connections.

## Features
- Distributed task distribution based on a shared file system
- Supports dynamic scaling
- Supports worker node heartbeat
- Supports manager node restart
- Supports set timeout for each task
- Supports set max_job and max_work_time for worker
- Pure Python implementation

## Install

```bash
pip install open-fleet
```

## Usage

See more examples in [./examples](./examples).

```python
# run_manager.py
from fleet.manager import Manager
from fleet.config.config import get_args

base_dir = "./share_dir"

def main():
    job_list = [1,2,3,4]
    args = get_args(f"--base_dir {base_dir}")
    manager = Manager(args=args, job_list=job_list)
    manager.run()

if __name__ == '__main__':
    main()
```
    
```python
# run_worker.py
from fleet.worker import Worker
from fleet.config.config import get_args

from run_manager import base_dir

def add_one(x, info):
    return {"status": "success", "result": x + 1}

def main():
    args = get_args(f"--base_dir {base_dir}")
    worker = Worker(args=args, job_func=add_one)
    worker.run()

if __name__ == '__main__':
    main()
```
