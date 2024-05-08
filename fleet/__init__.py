__TITLE__ = "fleet"
__VERSION__ = "v0.0.4"
__DESCRIPTION__ = "Fleet is a generic distributed task distribution framework based on a distributed file system."
__AUTHOR__ = "Shiyu Huang"
__version__ = __VERSION__

import platform

python_version_list = list(map(int, platform.python_version_tuple()))
assert python_version_list >= [
    3,
    8,
    0,
], (
    "Fleet requires Python 3.8 or newer, but your Python is"
    f" {platform.python_version()}"
)
