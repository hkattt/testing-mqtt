mod analyser;
mod publisher;
mod experiment;
mod mqtt_helper;

use rumqttc::QoS;

use std::{
    env, 
    sync::{Arc, Mutex}, 
    time::Duration
};

// MQQT topics
const INSTANCECOUNT_TOPIC: &str = "request/instancecount";
const QOS_TOPIC: &str           = "request/qos";
const DELAY_TOPIC: &str         = "request/delay";

// Output directory and files
const EXPERIMENT_DIR: &str      = "experiment-results";
const TOPIC_RESULTS_FILE: &str  = "topic-results.csv";
const SYS_RESULT_FILE: &str     = "sys-results.csv";

// Publisher send duration (seconds)
const SEND_DURATION: Duration   = Duration::from_secs(60); 

/// Entry point of the program. Spawns the analyser and publisher(s) needed to 
/// conduct the experiments. 
/// 
/// Receives uses input according to the below usage:
/// mqqt [-h <hostname>] [-p <port>] [-n <npublishers>] [-i <instancecount list>] [-q <qos list>] [-d <delay list>]
/// where each list argument is a list of comma-seperated values.
/// 
/// # Returns
/// A generic boxed error if any error is encountered.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Default program parameters
    // 1 to 5 pubishers
    // 3 different quality-of-service levels
    // 0ms, 1ms 2ms, and 4ms delay
    let mut hostname = String::from("localhost");
    let mut port: u16 = 1883;
    let mut npublishers: u8 = 5;
    let mut instancecounts: Vec<u8> = vec![1, 2, 3, 4, 5];
    let mut qoss: Vec<QoS> = vec![QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce];
    let mut delays: Vec<u64> = vec![0, 1, 2, 4];

    // Receives input arguments from the user 
    // Skips the first argument since it is the program name
    let mut args_iter = env::args().skip(1);
    while let Some(arg) = args_iter.next() {
        match arg.as_str() {
            // Hostname argument
            "-h" => {
                hostname = args_iter.next().ok_or("Missing hostname after -h")?;
            },
            // Port argument
            "-p" => {
                let port_str = args_iter.next().ok_or("Missing port after -p")?;
                port = match port_str.parse() {
                    Ok(port) => port,
                    Err(_) => {
                        eprintln!("Port must be a positve integer");
                        return Ok(());
                    } 
                }
            },
            // Number of publishers argument
            "-n" => {
                let npublishers_str = args_iter.next().ok_or("Missing number of publishers after -n")?;
                npublishers = match npublishers_str.parse() {
                    Ok(npublishers) => npublishers,
                    Err(_) => {
                        eprintln!("Number of publishers must be a positive integer");
                        return Ok(());
                    } 
                }
            },
            // Instancecount list
            "-i" => {
                let instancecounts_str = args_iter.next().ok_or("Missing instancecount list -i")?;
                instancecounts = instancecounts_str
                    .split(',')
                    .map(|s| s.parse().map_err(|_| "Instancecount must be a positive integer"))
                    .collect::<Result<Vec<_>, _>>()?;
            },
            // QoS list
            "-q" => {
                let qoss_str = args_iter.next().ok_or("Missing QoS list after -q")?;
                qoss = qoss_str
                    .split(',')
                    .filter_map(|s| mqtt_helper::str_to_qos(s))
                    .collect();
            },
            // Delay list
            "-d" => {
                let delays_str = args_iter.next().ok_or("Missing delay list after -d")?;
                delays = delays_str
                    .split(',')
                    .map(|s| s.parse().map_err(|_| "Delay must be a positive integer"))
                    .collect::<Result<Vec<_>, _>>()?;
            },
            // Invalid argument
            _ => {
                eprintln!("Usage: mqqt [-h <hostname>] [-p <port>] [-n <npublishers>] [-i <instancecount list>] [-q <qos list>] [-d <delay list>]");
                return Ok(());
            }
            
        }
    }

    println!(
        "STARTING EXPERIMENTS\n\
        \thostname: {}\n\
        \tport: {}\n\
        \tsend duration: {}s\n\
        \tmaximum number of publishers: {}\n\
        \tinstancecounts: {:?}\n\
        \tqoss: {:?}\n\
        \tdelays: {:?}\n",
        hostname,
        port,
        SEND_DURATION.as_secs(),
        npublishers,
        instancecounts,
        qoss,
        delays
    );

    // Arc is used for hostname since the String is shared across 
    // multiple threads
    let hostname = Arc::new(hostname);
    
    // Indicates whether the analyser is still conducting tests
    // Used to tell the publishers to stop
    let running = Arc::new(Mutex::new(true));

    // Counter values of all the publishers
    // Used to show the 'actual' counter value to the analyser
    let counters: Vec<Arc<Mutex<u64>>> = (0..npublishers)
        .map(|_| Arc::new(Mutex::new(0)))
        .collect();

    // Spawn publisher tasks
    let mut publisher_tasks = Vec::new();
    for publisher_index in 1..=npublishers {
        publisher_tasks.push(
            tokio::spawn(
                publisher::main_publisher(
                    publisher_index, 
                    Arc::clone(&hostname), 
                    port, 
                    Arc::clone(&running), 
                    Arc::clone(&counters[publisher_index as usize - 1])
                )
            )
        );
    }

    // Spawn analyser task
    let analyser_task = tokio::spawn(
        analyser::main_analyser(Arc::clone(&hostname), port, instancecounts, qoss, delays, counters)
    );

    // Wait for the analyser to finish
    let experiment_results = analyser_task.await.unwrap();

    // Signal for the publisher tasks to stop
    *running.lock().unwrap() = false;

    // Save experiment results
    if let Err(error) = experiment::save_experiment_results(experiment_results, TOPIC_RESULTS_FILE, SYS_RESULT_FILE) {
        eprintln!("Unable to save experiment results: {}\n", error);
        return Err(error)?;
    } else {
        println!("Saved experiment results to the {} directory\n", EXPERIMENT_DIR);
    }

    println!("EXPERIMENTS COMPLETED");

    Ok(())
}