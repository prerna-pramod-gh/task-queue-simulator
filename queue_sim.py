import multiprocessing
import time
import json
import logging
import random
from datetime import datetime
from typing import List, Dict
from queue import Empty

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Task:
    def __init__(self, task_id: str, description: str, priority: str = "low"):
        self.task_id = task_id
        self.description = description
        self.priority = priority  # "high" or "low"
        self.status = "queued"
        self.start_time = None
        self.end_time = None

def process_task(task: Task) -> None:
    """Simulate task processing with random duration."""
    try:
        task.status = "processing"
        task.start_time = datetime.now().isoformat()
        logger.info(f"Processing task {task.task_id}: {task.description}")
        
        # Simulate work (e.g., file conversion)
        processing_time = random.uniform(1, 5)  # Random 1-5 seconds
        time.sleep(processing_time)
        
        task.status = "completed"
        task.end_time = datetime.now().isoformat()
        logger.info(f"Completed task {task.task_id}")
    except Exception as e:
        task.status = "failed"
        logger.error(f"Task {task.task_id} failed: {str(e)}")

def save_logs(tasks: List[Task], filename: str = "task_logs.json") -> None:
    """Save task logs to JSON."""
    try:
        with open(filename, 'w') as f:
            json.dump([{
                "task_id": t.task_id,
                "description": t.description,
                "priority": t.priority,
                "status": t.status,
                "start_time": t.start_time,
                "end_time": t.end_time
            } for t in tasks], f, indent=4)
        logger.info(f"Saved logs to {filename}")
    except Exception as e:
        logger.error(f"Failed to save logs: {str(e)}")

def worker(task_queue: multiprocessing.Queue, worker_id: int) -> None:
    """Worker process to handle tasks."""
    while True:
        try:
            task = task_queue.get(timeout=2)  # Timeout to allow graceful exit
            logger.info(f"Worker {worker_id} picked task {task.task_id}")
            process_task(task)
        except Empty:
            logger.info(f"Worker {worker_id} exiting: no tasks left")
            break
        except Exception as e:
            logger.error(f"Worker {worker_id} error: {str(e)}")

def main():
    # Read tasks from file
    tasks: List[Task] = []
    try:
        with open("sample_tasks.txt", "r") as f:
            for i, line in enumerate(f):
                desc = line.strip()
                if desc:
                    priority = "high" if i % 2 == 0 else "low"  # Alternate priorities
                    tasks.append(Task(f"task_{i+1}", desc, priority))
    except FileNotFoundError:
        logger.error("sample_tasks.txt not found")
        return

    if not tasks:
        logger.info("No tasks to process")
        return

    # Sort tasks by priority (high first)
    tasks.sort(key=lambda x: x.priority != "high")

    # Initialize task queue
    task_queue = multiprocessing.Queue()
    for task in tasks:
        task_queue.put(task)

    # Start workers
    num_workers = min(multiprocessing.cpu_count(), 4)  # Cap at 4 workers
    logger.info(f"Starting {num_workers} workers")
    processes = [
        multiprocessing.Process(target=worker, args=(task_queue, i))
        for i in range(num_workers)
    ]

    # Run and clean up
    for p in processes:
        p.start()
    for p in processes:
        p.join()

    # Save logs
    save_logs(tasks)

if __name__ == "__main__":
    main()
