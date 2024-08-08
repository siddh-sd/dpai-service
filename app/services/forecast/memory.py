import psutil
import logging

logger = logging.getLogger(__name__)

def get_memory_usage(tenant_id):
    # memory_info = psutil.virtual_memory()
    # logger.info(f"avaible_memory {tenant_id} {memory_info.available / (1024 * 1024)} MB || {memory_info.available / (1024 * 1024 * 1024)} GB")
    # used_memory_mb = memory_info.used / (1024 ** 2)
    # return used_memory_mb

    process = psutil.Process()  # Get the current process
    memory_info = process.memory_info()
    return memory_info.rss/ (1024 ** 2)