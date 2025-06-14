# dag_examples.py

sample_dags = {
    "example_dag_1": {
        "A": ["B", "C"],
        "B": ["D"],
        "C": ["D"],
        "D": []
    },
    "example_dag_2": {
        "X": ["Y"],
        "Y": ["Z"],
        "Z": []
    }
}