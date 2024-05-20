#!/bin/bash

# Script: start.sh
# Description: Start the Mosquitto broker service and display its status.
# Usage: sudo ./start.sh

# Start Mosquitto broker
systemctl start mosquitto

# Display the status of the broker
systemctl status mosquitto
