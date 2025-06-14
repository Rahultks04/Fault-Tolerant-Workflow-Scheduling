import streamlit as st
import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd
import json
import random

# Wide layout
st.set_page_config(layout="wide")
st.title("Fault Tolerant Workflow Scheduler with DAG")

# Sample predefined DAGs
predefined_dags = {
    "Simple DAG 1": {
        "A": [],
        "B": ["A"],
        "C": ["A"],
        "D": ["B", "C"]
    },
    "Linear Chain": {
        "T1": [],
        "T2": ["T1"],
        "T3": ["T2"],
        "T4": ["T3"]
    }
}

# Sidebar: Choose or upload DAG
st.sidebar.header("Workflow Input")
dag_option = st.sidebar.selectbox("Choose a predefined DAG or upload", ["Choose predefined", "Upload JSON"])

dag = {}

if dag_option == "Choose predefined":
    selected = st.sidebar.selectbox("Select a DAG", list(predefined_dags.keys()))
    dag = predefined_dags[selected]
else:
    uploaded_file = st.sidebar.file_uploader("Upload a DAG JSON file", type=["json"])
    if uploaded_file:
        try:
            dag = json.load(uploaded_file)
            st.sidebar.success("DAG loaded successfully!")
        except Exception as e:
            st.sidebar.error(f"Error loading JSON: {e}")

# Continue only if DAG is loaded
if dag:
    st.subheader("Workflow DAG")

    # Convert DAG to NetworkX graph
    G = nx.DiGraph()
    for task, deps in dag.items():
        G.add_node(task)
        for dep in deps:
            G.add_edge(dep, task)

    # Visualize DAG
    fig, ax = plt.subplots(figsize=(10, 6))
    nx.draw(G, with_labels=True, node_color='skyblue', node_size=2000, font_size=12, ax=ax)
    st.pyplot(fig)

    # Simulate execution with fault tolerance
    st.subheader("Task Execution Log with Fault Tolerance")

    vms = ["VM-1", "VM-2", "VM-3"]
    execution_log = []
    executed = set()

    for task in nx.topological_sort(G):
        dependencies = list(G.predecessors(task))
        if all(dep in executed for dep in dependencies):
            assigned_vm = random.choice(vms)
            status = random.choice(["success", "fail"])
            execution_log.append({"Task": task, "Attempt": 1, "VM": assigned_vm, "Status": status})

            if status == "fail":
                alt_vm = random.choice([vm for vm in vms if vm != assigned_vm])
                execution_log.append({"Task": task, "Attempt": 2, "VM": alt_vm, "Status": "success"})
                executed.add(task)
            else:
                executed.add(task)
        else:
            execution_log.append({"Task": task, "Attempt": 0, "VM": "-", "Status": "waiting"})

    df = pd.DataFrame(execution_log)
    st.dataframe(df)

else:
    st.warning("Please select or upload a DAG to continue.")