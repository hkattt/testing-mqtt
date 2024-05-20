# COMP3310 - Assignment 3: Testing MQTT

Hugo Kat: u7287091

## Requirements
This MQTT network analyser was implemented in Rust. See the [Installation section](https://doc.rust-lang.org/book/ch01-01-installation.html) of The Rust Programming Language book for installation steps. 

The project uses the following crates:
* `rumqttc`: For MQQT library.
* `tokio`: For aysnchronous runtime and task spawning.
* `csv`: For saving data to `.csv` files.
* `chrono`: For date-time functionality.
* `debug_print`: For print functions which only trigger in debug mode.

The MQTT network analyser was primarily tested locally using the Mosquitto broker on an Ubuntu Linux virtual machine. See the [Mosquitto Download](https://mosquitto.org/download/) page for further details on setting up the Mosquitto broker. We used the below Mosquitto configuration file to prevent the broker from previous experiments. 

```
# Place your local configuration in /etc/mosquitto/conf.d/
#
# A full description of the configuration file is at
# /usr/share/doc/mosquitto/examples/mosquitto.conf.example

persistence false
persistence_location /var/lib/mosquitto/

log_dest file /var/log/mosquitto/mosquitto.log

include_dir /etc/mosquitto/conf.d
```

The project uses Python to graph the experiment results. See the [Python Download](https://www.python.org/downloads/) for further details on installing Python. Our Python script requires the `numpy`, `pandas`, and `matplotlib` libraries which can be installed using the `pip` package manager. 

## Usage

The usage for the program is:
```
mqqt [-h <hostname>] [-p <port>] [-n <npublishers>] [-i <instancecount list>] [-q <qos list>] [-d <delay list>]
```
Where
* `-h` specifies the hostname of the MQQT broker
* `-p` specifies the port that the MQQT broker is on
* `-n` specifies the number of publishers
* `-i` specifies the instancecounts to use during testing as a comma seperated list
* `-q` specifies the quality-of-service levels (0, 1, and 2) to use during testing as a commas seperated list
* `-d` specifies the delay to use during testing as a comma seperated list

with default values `hostname=localhost`, `port=1883`, `npublishers=5`, `instancecount list=1,2,3,4,5`, `qos list=0,1,2`, and `delay list=0,1,2,4`.

To run the program in debug mode use
```
cargo run -- [-h <hostname>] [-p <port>] [-n <npublishers>] [-i <instancecount list>] [-q <qos list>] [-d <delay list>]
```
in the root directory. In debug mode, the program will print additional information and error messages. To run the program in release mode use 
```
cargo run --release -- [-h <hostname>] [-p <port>] [-n <npublishers>] [-i <instancecount list>] [-q <qos list>] [-d <delay list>]
```
This will only print an overview of each experiment. Once all the experiments have finished running, the results will be saved to the `experiment-results` folder. The `topic-results.csv` file contains the statistics collected and computed by the analyser. The `sys-results.csv` file contains statistics received from the Mosquitto broker `$SYS/#` measurements.  

## Scripts
We have included several scripts in the `scripts` folder to ease usage with the Mosquitto broker. Full details of their usage can be found in the scripts themselves. The `start.sh` and `stop.sh` scripts should be used to start and stop the Mosquitto broker between tests. 

The `results.py` script can be used *after* completing an experiment to visualise the results. The figures are saved to the `figures` directory. 

## Project Structure
```
├── Cargo.lock
├── Cargo.toml
├── README.md
├── experiment-results
├── figures
├── scripts
│   ├── delete-log.sh
│   ├── log.sh
│   ├── results.py
│   ├── start.sh
│   ├── status.sh
│   └── stop.sh
└── src
    ├── analyser.rs
    ├── experiment.rs
    ├── main.rs
    ├── mqqt_helper.rs
    └── publisher.rs
```