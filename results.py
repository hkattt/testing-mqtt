# Plotting libraries
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import os

OUTPUT_DIR = "figures/"
TOPIC_RESULTS_FILE_PATH = "experiment-results/topic-results.csv"
SYS_RESULTS_FILE_PATH = "experiment-results/sys-results.csv"

# Create output directory if it does not exist already
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

topic_results = pd.read_csv(TOPIC_RESULTS_FILE_PATH)
sys_results = pd.read_csv(SYS_RESULTS_FILE_PATH)

# =======================================================================================

instance_groups = topic_results.groupby(["instancecount"])
nplots = len(instance_groups)
cols = 3
rows = nplots % cols

fig, axs = plt.subplots(rows, cols, figsize=(15, 10))
fig.suptitle("Message Rate vs Delay for Different QoS and Instance Counts")

axs = axs.flatten()

handles_labels = []

for i, (instancecount, group) in enumerate(instance_groups):
    ax = axs[i]
    for analyser_qos in group["analyser-qos"].unique():
        for publisher_qos in group["publisher-qos"].unique():
            subset = group[(group["analyser-qos"] == analyser_qos) & (group["publisher-qos"] == publisher_qos)]
            handle, = ax.plot(subset["delay"], subset["message-rate"], marker="o")
            if i == 0:
                handles_labels.append((handle, f"Analyser QoS={analyser_qos} and Publisher QoS={publisher_qos}"))
    ax.set_xlabel("Delay (ms)")
    ax.set_ylabel("Message Rate (message per second)")
    ax.set_title(f"Instance Count = {instancecount[0]}")
    ax.grid(True)

handles, labels = zip(*handles_labels)

fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.95), ncol=3)
# Hide unused subplots
for ax in axs[nplots:]:
    ax.set_visible(False)

plt.tight_layout(rect=[0, 0, 1, 0.9])

plt.savefig(OUTPUT_DIR + "message-rate-plot")
plt.close()

# ==================================================================================================

instance_groups = topic_results.groupby(["instancecount"])
nplots = len(instance_groups)
cols = 3
rows = nplots % cols

fig, axs = plt.subplots(rows, cols, figsize=(15, 10))
fig.suptitle("Median Inter-Message Gap vs Delay for Different QoS and Instance Counts")

axs = axs.flatten()

handles_labels = []

for i, (instancecount, group) in enumerate(instance_groups):
    ax = axs[i]
    for analyser_qos in group["analyser-qos"].unique():
        for publisher_qos in group["publisher-qos"].unique():
            subset = group[(group["analyser-qos"] == analyser_qos) & (group["publisher-qos"] == publisher_qos)]
            handle, = ax.plot(subset["delay"], subset["inter-message-gap"], marker="o")
            if i == 0:
                handles_labels.append((handle, f"Analyser QoS={analyser_qos} and Publisher QoS={publisher_qos}"))
    ax.set_xlabel("Delay (ms)")
    ax.set_ylabel("Inter-Message Gap (ms)")
    ax.set_title(f"Instance Count = {instancecount[0]}")
    ax.grid(True)

handles, labels = zip(*handles_labels)

fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.95), ncol=3)
# Hide unused subplots
for ax in axs[nplots:]:
    ax.set_visible(False)

plt.tight_layout(rect=[0, 0, 1, 0.9])

plt.savefig(OUTPUT_DIR + "inter-message-gap-plot")
plt.close()

# ==================================================================================================

instance_groups = topic_results.groupby(["instancecount"])
nplots = len(instance_groups)
cols = 3
rows = nplots % cols

fig, axs = plt.subplots(rows, cols, figsize=(15, 10))
fig.suptitle("Loss Rate vs Delay for Different QoS and Instance Counts")

axs = axs.flatten()

handles_labels = []

for i, (instancecount, group) in enumerate(instance_groups):
    ax = axs[i]
    for analyser_qos in group["analyser-qos"].unique():
        for publisher_qos in group["publisher-qos"].unique():
            subset = group[(group["analyser-qos"] == analyser_qos) & (group["publisher-qos"] == publisher_qos)]
            handle, = ax.plot(subset["delay"], subset["loss-rate"], marker="o")
            if i == 0:
                handles_labels.append((handle, f"Analyser QoS={analyser_qos} and Publisher QoS={publisher_qos}"))
    ax.set_xlabel("Delay (ms)")
    ax.set_ylabel("Loss Rate (proportion)")
    ax.set_title(f"Instance Count = {instancecount[0]}")
    ax.grid(True)

handles, labels = zip(*handles_labels)

fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.95), ncol=3)
# Hide unused subplots
for ax in axs[nplots:]:
    ax.set_visible(False)

plt.tight_layout(rect=[0, 0, 1, 0.9])

plt.savefig(OUTPUT_DIR + "loss-rate-plot")
plt.close()