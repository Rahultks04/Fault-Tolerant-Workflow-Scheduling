import time
import threading
import random
import streamlit as st

def simulate_task(task_id, duration=2):
    """ Simulate task execution with random failure """
    st.write(f"Running {task_id}")
    time.sleep(duration)
    if random.random() < 0.2:  # 20% chance to fail
        raise Exception(f"Task {task_id} failed")
    return f"{task_id} completed"

def run_with_replication(task_id):
    results = []

    def wrapper():
        try:
            result = simulate_task(task_id)
            results.append(result)
        except Exception as e:
            results.append(str(e))

    threads = [threading.Thread(target=wrapper) for _ in range(2)]
    for t in threads: t.start()
    for t in threads: t.join()

    for r in results:
        if "completed" in r:
            return r
    return f"{task_id} failed after replication"

def schedule_dag(dag):
    completed = set()
    while len(completed) < len(dag):
        for task, deps in dag.items():
            if task in completed:
                continue
            if all(dep in completed for dep in deps):
                try:
                    result = run_with_replication(task)
                    st.success(result)
                    completed.add(task)
                except Exception:
                    st.error(f"{task} failed even after retry")