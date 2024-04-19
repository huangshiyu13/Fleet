import time

def format_time(time_second: float)->str:
    # 根据时间的长度返回不同单位的字符串
    if time_second < 60:
        return f"{time_second:.2f} sec"
    elif time_second < 3600:
        return f"{time_second / 60:.2f} min"
    elif time_second < 86400:
        return f"{time_second / 3600:.2f} h"
    else:
        return f"{time_second / 86400:.2f} day"


class TimeTracker:
    def __init__(self, total_tasks: int):
        assert total_tasks > 0, "task number must larger than zero!"
        self.total_tasks = total_tasks
        self.reset()

    def reset(self):
        self.finished_tasks = 0
        self.start_time = time.time()

    def update(self, task_num: int = 1):
        self.finished_tasks += task_num
        self.current_time = time.time()

    def set(self, finished_tasks: int):
        self.finished_tasks = finished_tasks
        self.current_time = time.time()

    @property
    def est(self) -> str:
        if self.finished_tasks == 0:
            return "Unknown"
        elapsed_time = self.current_time - self.start_time
        remaining_tasks = self.total_tasks - self.finished_tasks
        time_usage_per_task = elapsed_time / self.finished_tasks
        remaining_time = remaining_tasks * time_usage_per_task

        return format_time(remaining_time)

    @property
    def speed(self) -> str:
        if self.finished_tasks == 0:
            return "Unknown"
        elapsed_time = self.current_time - self.start_time
        time_usage_per_task = elapsed_time / self.finished_tasks

        if time_usage_per_task < 1:
            return f"{1 / time_usage_per_task:.2f} item/s"
        else:
            return f"{time_usage_per_task:.2f} s/item"

    @property
    def elapsed(self):
        return format_time(time.time()-self.start_time)

    @property
    def summary(self)->str:
        return f"Elapsed: {self.elapsed} EST: {self.est} Speed: {self.speed}"
def test_time_tracker():
    total_jobs = 10000000
    tracker = TimeTracker(total_jobs)
    for i in range(total_jobs):
        time.sleep(0.1)
        tracker.update()
        print(tracker.summary)


if __name__ == '__main__':
    test_time_tracker()