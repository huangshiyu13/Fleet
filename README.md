# Fleet

Fleet is a generic distributed task distribution framework based on a distributed file system. Task distribution frameworks like Ray and Celery require network connections for communication, which makes them difficult to use in clusters with poor network conditions. Fleet is a distributed framework based on a shared file system, independent of any network communication, allowing for task distribution among nodes without any network connections.

## Install

```bash
pip install -e .
```
