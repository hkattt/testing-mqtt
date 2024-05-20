#!/bin/bash

# Script: stop.sh
# Description: Stop the Mosquitto broker service and display its status.
# Usage: sudo ./stop.sh

# Stop Mosquitto broker
systemctl stop mosquitto

# Display the status of the broker
systemctl status mosquitto