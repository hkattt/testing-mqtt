mod analyser;
mod publisher;
mod experiment;

use std::{env, sync::{Arc, Mutex}, time::Duration};
use std::fmt::Debug;

use rumqttc::{AsyncClient, ClientError, EventLoop, MqttOptions, Publish, QoS};
use::debug_print::{debug_println, debug_eprintln};

// MQQT topics
const INSTANCECOUNT_TOPIC: &str = "request/instancecount";
const QOS_TOPIC: &str           = "request/qos";
const DELAY_TOPIC: &str         = "request/delay";

const EXPERIMENT_DIR: &str      = "experiment-results";
const TOPIC_RESULTS_FILE: &str  = "topic-results.csv";
const SYS_RESULT_FILE: &str     = "sys-results.csv";

// Publisher send duration (seconds)
const SEND_DURATION: Duration   = Duration::from_secs(10); 

/**
 * TODO:
 * 1. Make the Analyser start EXACTLY when the publishers finish
 *      - Just make the publisher send everything to begin with?
 * 2. Make the Publisher actually end
 * 3. The program should go VERY quickly at 0ms delay and qos = 0
 * 4. Figure out why the first experiments literally receive nothing?
 */

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Default program parameters
    let mut hostname = String::from("localhost");
    let mut port: u16 = 1883;
    let mut npublishers: u8 = 5;

    // 1 to 5 publishers
    let mut instancecounts: Vec<u8> = vec![1, 2, 3, 4, 5];
    // 3 different quality-of-service levels
    let mut qoss: Vec<QoS> = vec![QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce];
    // 0ms, 1ms, 2ms, 4ms message delay
    let mut delays: Vec<u64> = vec![0, 1, 2, 4];

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
                    .filter_map(|s| str_to_qos(s))
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
        \tsend duration: {} s\n\
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

    let hostname = Arc::new(hostname);
    let running = Arc::new(Mutex::new(true));

    let counters: Vec<Arc<Mutex<u64>>> = (0..npublishers)
        .map(|_| Arc::new(Mutex::new(0)))
        .collect();

    println!("Spawning publisher task(s)\n");
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

    println!("Spawning analyser task\n");
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

fn create_mqtt_conn(client_id: &str, hostname: &str, port: u16, keep_alive: Duration) -> (AsyncClient, EventLoop) {
    // Create MQTT options
    let mut options = MqttOptions::new(client_id, hostname, port);
    options.set_keep_alive(keep_alive);
    options.set_clean_session(true);

    // Create MQTT client and connection 
    AsyncClient::new(options, 10)
}

async fn subscribe_to_topics<S>(client: &AsyncClient, client_id: &str, qos: QoS, topics: &[S]) -> Result<(), ClientError> 
where 
    S: AsRef<str> + Debug,
{
    for topic in topics {
        let topic_str = topic.as_ref();
        if let Err(error) = client.subscribe(topic_str, qos).await {
            // TODO: Replace with debug prints
            debug_eprintln!("{} failed to subscribe to topic {} with error: {}", client_id, topic_str, error);
            return Err(error);
        } else {
            // TODO: Replace with debug prints
            debug_println!("{} subscribed to topic: {}", client_id, topic_str);
        }
    }
    Ok(())
}

fn publisher_topic_string(instance: u8, qos: QoS, delay: u64) -> String {
    format!("counter/{}/{}/{}", instance, qos_to_u8(qos), delay)
}

fn publisher_topic_instance(publisher_topic: &str) -> Option<usize> {
    let parts: Vec<&str> = publisher_topic.split('/').collect();

    if let Some(instance_str) = parts.get(1) {
        if let Ok(instance) = instance_str.parse::<usize>() {
            return Some(instance);
        } else {
            debug_eprintln!("The second part of the publisher topic string cannot be parsed into an integer");
            return None;
        }
    } else {
        debug_eprintln!("The publisher topic string has no second part");
        return None;
    }
}

fn qos_to_u8(qos: QoS) -> u8 {
    match qos {
        QoS::AtMostOnce  => 0, 
        QoS::AtLeastOnce => 1,
        QoS::ExactlyOnce => 2,
    }
}

fn u8_to_qos(qos: u8) -> Option<QoS> {
    match qos {
        0 => Some(QoS::AtMostOnce),
        1 => Some(QoS::AtLeastOnce),
        2 => Some(QoS::ExactlyOnce),
        _ => None, // Invalid QoS
    }
}

fn str_to_qos(qos: &str) -> Option<QoS> {
    match qos {
        "0" => Some(QoS::AtMostOnce),
        "1" => Some(QoS::AtLeastOnce),
        "2" => Some(QoS::ExactlyOnce),
        _ => None, // Invalid QoS
    }
}

fn bytes_to_u64(publish: &Publish) -> u64 {
    let payload = &publish.payload.to_vec();
    let mut array = [0u8; 8];
    let len = payload.len().min(8);
    array[..len].copy_from_slice(&payload[..len]);
    u64::from_be_bytes(array)
}

fn utf8_to_u64(publish: &Publish) -> u64 {
    let payload = &publish.payload;
    let payload_str = std::str::from_utf8(&payload).unwrap();
    payload_str.parse::<u64>().unwrap()
}