import argparse
import time
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.datasets import make_classification

def calculate(process, n_samples,job):
    # Generate a synthetic dataset for classification
    X, y = make_classification(n_samples=10000, n_features=20, random_state=42)

    # Train a Random Forest classifier
    classifier = RandomForestClassifier(n_estimators=1000)
    classifier.fit(X, y)

    start_time = time.time()
    # Perform predictions
    X_test, _ = make_classification(n_samples=n_samples, n_features=20, random_state=42)
    predictions = classifier.predict(X_test)
    len_predictions = len(predictions)
    mean_predictions = np.mean(predictions)
    print(f"length of predictions {len_predictions}")
    print(f"mean of predictions {mean_predictions}")

    end_time = time.time()
    execution_time = end_time - start_time

    return process, job, predictions, execution_time

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process ID argument')
    parser.add_argument('-p',"--process_id", type=int, help='ID of the process')
    parser.add_argument('-jo',"--job_id",type=int, help="ID of the iteration")
    args = parser.parse_args()

    process_id = args.process_id
    job_id = args.job_id
    num_processes = 1
    n_samples = 1000000

    # Calculate the number of samples per process
    samples_per_process = n_samples // num_processes
    #print(process_id, iteration_id)

    if process_id < 1 or process_id > num_processes:
        print(f"Invalid process ID. Process ID must be between 1 and {num_processes}.")
        exit(1)

    process, job, predictions, time_taken = calculate(process_id, samples_per_process, job_id)

    print(f"Process {process}, Job {job}")
    print("Predictions:", predictions)
    print("Calculation Time:", time_taken, "seconds")

