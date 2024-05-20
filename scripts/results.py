# Plotting libraries
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import os

OUTPUT_DIR = "figures/"
TOPIC_RESULTS_FILE_PATH = "experiment-results/topic-results.csv"
SYS_RESULTS_FILE_PATH = "experiment-results/sys-results.csv"

def create_instance_plots(data, ycol, title, ylabel, file_name):
    # Create a seperate plot for each instancecount
    instance_groups = data.groupby(["instancecount"])

    # Number of subplots
    nplots = len(instance_groups)

    cols = 3
    rows = nplots % cols

    fig, axs = plt.subplots(rows, cols, figsize=(15, 10))
    fig.suptitle(f"{title} vs Delay for Different QoS and Instance Counts")

    axs = axs.flatten()

    # Store figure labels
    handles_labels = []

    for i, (instancecount, group) in enumerate(instance_groups):
        ax = axs[i]

        for analyser_qos in group["analyser-qos"].unique():
            for publisher_qos in group["publisher-qos"].unique():
                # Extract subset of results with current analyser QoS and publisher QoS
                subset = group[(group["analyser-qos"] == analyser_qos) & (group["publisher-qos"] == publisher_qos)]
                # Each combination may have multiple sub-experiments (i.e. different instances)
                # We average their results
                averaged_subset = subset.groupby("delay")[ycol].mean().reset_index()
                # Create subplot
                handle, = ax.plot(averaged_subset["delay"], averaged_subset[ycol], marker="o")
                # Add plot labels (every subplot has the same labels)
                if i == 0:
                    handles_labels.append((handle, f"Analyser QoS={analyser_qos} and Publisher QoS={publisher_qos}"))
        ax.set_xlabel("Delay (ms)")
        ax.set_ylabel(ylabel)
        ax.set_title(f"Instance Count = {instancecount[0]}")
        ax.grid(True)

    handles, labels = zip(*handles_labels)

    fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.95), ncol=3)
    
    # Hide unused subplots
    for ax in axs[nplots:]:
        ax.set_visible(False)

    # Adjust plot layout
    plt.tight_layout(rect=[0, 0, 1, 0.9])

    # Save the figure
    plt.savefig(OUTPUT_DIR + file_name)

    plt.close()

if __name__ == "__main__":
    # Create output directory if it does not exist already
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    topic_results = pd.read_csv(TOPIC_RESULTS_FILE_PATH)
    sys_results = pd.read_csv(SYS_RESULTS_FILE_PATH)

    # Show all rows
    pd.set_option("display.max_rows", None)
    # Show all columns
    pd.set_option("display.max_columns", None) 
    # Prevent line wrapping
    pd.set_option("display.expand_frame_repr", False)

    create_instance_plots(topic_results, "message-rate", "Message Rate", "Message Rate (msg/second)", "message-rate-plot.png")
    create_instance_plots(topic_results, "loss-rate", "Loss Rate", "Loss Rate (proportion of messages)", "loss-rate-plot.png")
    create_instance_plots(topic_results, "out-of-order-rate", "Out-of-order Rate", "Out-of-order Rate (proportion of messages)", "out-of-order-rate-plot.png")
    create_instance_plots(topic_results, "inter-message-gap", "Median Inter-message Gap", "Median Inter-message Gap (ms)", "median-inter-message-plot.png")
    
    create_instance_plots(sys_results, "avg-heap-size", "Average Heap Size", "Average Heap Size (bytes)", "avg-heap-size-plot.png")