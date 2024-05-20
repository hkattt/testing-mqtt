#!/bin/bash

# Script: delete-log.sh
# Description: Delete Mosquitto log
# Usage: sudo ./delete-log.sh

# Delete Mosquitto log
truncate -s 0 /var/log/mosquitto/mosquitto.log