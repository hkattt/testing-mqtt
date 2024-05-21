"""
Script Name: plot_normal.py
Description: Used to plot results received from the normal/standard local broker experiments
Usage:
    python3 plot_normal.py (-t normal-topic-results.csv) (-s normal-sys-results.csv)
"""

# Plotting libraries
import pandas as pd
import matplotlib.pyplot as plt

import os
import argparse

INPUT_DIR          = "experiment-results/"
OUTPUT_DIR         = "figures/"

def create_diff_cols(data, colname):
    diff_colname = colname + "-diff"
    data[diff_colname] = data[colname].diff()
    data.loc[0, diff_colname] = data.loc[0, colname]

def create_instance_plots(data, ycol, title, ylabel, file_name, ignore_zeros=False):
    # Create a seperate plot for each instancecount
    instance_groups = data.groupby(["instancecount"])

    # Number of subplots
    nplots = len(instance_groups)

    cols = 3
    rows = nplots % cols

    fig, axs = plt.subplots(rows, cols, figsize=(15, 10))
    fig.suptitle(f"{title} vs Delay for Different QoS and Instance Counts", fontsize=20, fontweight="bold")

    if ignore_zeros:
        fig.text(0.5, 0.93, "NOTE: Only y-values greater than 0 are plotted", fontsize=12, ha="center")

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

                if ignore_zeros:
                    averaged_subset = averaged_subset[averaged_subset[ycol] > 0]

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

    if ignore_zeros:
        fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.92), ncol=3)
    else:
        fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.94), ncol=3)

    # Hide unused subplots
    for ax in axs[nplots:]:
        ax.set_visible(False)

    # Adjust plot layout
    plt.tight_layout(rect=[0, 0, 1, 0.9])

    # Save the figure
    plt.savefig(OUTPUT_DIR + file_name)

    plt.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--topic_results_file_name", type=str, default="topic-results.csv", help="Topic Results Source CSV")
    parser.add_argument("-s", "--sys_results_file_name", type=str, default="sys-results.csv", help="Topic Results Source CSV")
    
    args = parser.parse_args()

    # Create output directory if it does not exist already
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    topic_results = pd.read_csv(INPUT_DIR + args.topic_results_file_name)
    sys_results = pd.read_csv(INPUT_DIR + args.sys_results_file_name)

    # Show all rows
    pd.set_option("display.max_rows", None)
    # Show all columns
    pd.set_option("display.max_columns", None) 
    # Prevent line wrapping
    pd.set_option("display.expand_frame_repr", False)

    # Add difference columns
    create_diff_cols(sys_results, "npub-msg-recv")
    create_diff_cols(sys_results, "npub-msg-sent")
    create_diff_cols(sys_results, "npub-msg-dropped")

    # Topic results graphs

    create_instance_plots(
        topic_results, 
        "message-rate", 
        "Message Rate", 
        "Message Rate (msg/second)", 
        "message-rate-plot.png",
        ignore_zeros=True
    )
    
    create_instance_plots(
        topic_results, 
        "loss-rate", 
        "Loss Rate", 
        "Loss Rate (proportion of messages)", 
        "loss-rate-plot.png",
    )

    create_instance_plots(
        topic_results, 
        "out-of-order-rate", 
        "Out-of-order Rate", 
        "Out-of-order Rate (proportion of messages)", 
        "out-of-order-rate-plot.png",
        ignore_zeros=True
    )

    create_instance_plots(
        topic_results, 
        "inter-message-gap", 
        "Median Inter-message Gap", 
        "Median Inter-message Gap (ms)", 
        "median-inter-message-plot.png",
        ignore_zeros=True
    )
    
    # $SYS results graphs

    create_instance_plots(
        sys_results, 
        "avg-heap-size", 
        "Average Heap Size", 
        "Average Heap Size (bytes)", 
        "avg-heap-size-plot.png",
        ignore_zeros=True
    )

    create_instance_plots(
        sys_results, 
        "nconnected-clients", 
        "Number of Connected Clients", 
        "Number of Connected Clients", 
        "nconnected-clients-plot.png",
        ignore_zeros=True
    )

    create_instance_plots(
        sys_results, 
        "npub-msg-recv-diff", 
        "Number of Publisher Messages Received", 
        "Number of Publisher Messages Received", 
        "npub-msgs-recv.png",
        ignore_zeros=True
    )

    create_instance_plots(
        sys_results, 
        "npub-msg-sent-diff", 
        "Number of Publisher Messages Sent", 
        "Number of Publisher Messages Sent", 
        "npub-msgs-sent.png",
        ignore_zeros=True
    )

    create_instance_plots(
        sys_results, 
        "npub-msg-dropped-diff", 
        "Number of Publisher Messages Dropped", 
        "Number of Publisher Messages Dropped", 
        "npub-msgs-dropped.png",
        ignore_zeros=True
    )

if __name__ == "__main__":
    main()