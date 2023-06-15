import argparse
import time
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.datasets import make_classification

def calculate(n_samples, process):
    start_time = time.time()

    # Generate a synthetic dataset for classification
    X, y = make_classification(n_samples=10000, n_features=20, random_state=42)

    # Train a Random Forest classifier
    classifier = RandomForestClassifier(n_estimators=1000)
    classifier.fit(X, y)

    # Perform predictions
    X_test, _ = make_classification(n_samples=n_samples, n_features=20, random_state=42)
    predictions = classifier.predict(X_test)
    print(len(predictions))
    print(np.mean(predictions))

    end_time = time.time()
    execution_time = end_time - start_time

    return process, predictions, execution_time

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process ID argument')
    parser.add_argument('-p',"--process_id", type=int, help='ID of the process')
    args = parser.parse_args()
    process_id = args.process_id
    num_processes = 2
    n_samples = 10000

    # Calculate the number of samples per process
    samples_per_process = n_samples // num_processes

    if process_id < 1 or process_id > num_processes:
        print(f"Invalid process ID. Process ID must be between 1 and {num_processes}.")
        exit(1)

    process, predictions, time_taken = calculate(samples_per_process, process_id)

    print(f"Process {process}:")
    print("Predictions:", predictions)
    print("Calculation Time:", time_taken, "seconds")

