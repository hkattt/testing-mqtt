"""
Script Name: plot_many.py
Description: Used to plot results received from the 10 or more publishers experiment
Usage:
    python3 plot_many.py (-t many-topic-results.csv) (-s many-sys-results.csv)
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

def create_bar(data, ycol, title, ylabel, file_name):
    averaged_col = data.groupby("instancecount")[ycol].mean().reset_index()

    plt.bar(data["instancecount"].unique(), averaged_col[ycol])

    plt.title(f"{title} Bar Graph", fontsize=15, fontweight="bold")
    plt.xlabel("Instance Count")
    plt.ylabel(f"{ylabel}")

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

    # Topic results graphs

    create_bar(
        topic_results, 
        "message-rate", 
        "Message Rate", 
        "Message Rate (msg/second)", 
        "message-rate-plot.png",
    )

    create_bar(
        sys_results, 
        "avg-heap-size", 
        "Average Heap Size", 
        "Average Heap Size (bytes)", 
        "avg-heap-size-plot.png",
    )

if __name__ == "__main__":
    main()