mod analyser;
mod publisher;
mod experiment;

use std::{sync::{Arc, Mutex}, time::Duration};
use std::fmt::Debug;

use rumqttc::{AsyncClient, ClientError, EventLoop, MqttOptions, QoS};
use::debug_print::{debug_println, debug_eprintln};

// Broker details
const HOSTNAME: &str            = "localhost";
const PORT: u16                 = 1883;

// MQQT topics
const INSTANCECOUNT_TOPIC: &str = "request/instancecount";
const QOS_TOPIC: &str           = "request/qos";
const DELAY_TOPIC: &str         = "request/delay";

const EXPERIMENT_DIR: &str      = "experiment-results";
const TOPIC_RESULTS_FILE: &str  = "topic-results.csv";
const SYS_RESULT_FILE: &str     = "sys-results.csv";

// Publisher send duration (seconds)
const SEND_DURATION: Duration   = Duration::from_secs(1); 
// Maximum number of publishers 
const NPUBLISHERS: u8           = 5;

/**
 * TODO:
 * 1. Make the Analyser start EXACTLY when the publishers finish
 *      - Just make the publisher send everything to begin with?
 * 2. Make the Publisher actually end
 * 3. The program should go VERY quickly at 0ms delay and qos = 0
 * 4. Figure out why the first experiments literally receive nothing?
 */

#[tokio::main]
async fn main() {
    println!(
        "STARTING EXPERIMENTS
        \thostname: {}
        \tport: {}
        \tsend duration: {} s
        \tmaximum number of publishers: {}\n",
        HOSTNAME,
        PORT,
        SEND_DURATION.as_secs(),
        NPUBLISHERS
    );

    let running = Arc::new(Mutex::new(true));

    let counters: Vec<Arc<Mutex<u64>>> = (0..NPUBLISHERS)
        .map(|_| Arc::new(Mutex::new(0)))
        .collect();

    println!("Spawning publisher task(s)\n");
    let mut publisher_tasks = Vec::new();
    for publisher_index in 1..=NPUBLISHERS {
        publisher_tasks.push(
            tokio::spawn(
                publisher::main_publisher(
                    publisher_index, 
                    HOSTNAME, 
                    PORT, 
                    Arc::clone(&running), 
                    Arc::clone(&counters[publisher_index as usize - 1])
                )
            )
        );
    }

    println!("Spawning analyser task\n");
    let analyser_task = tokio::spawn(
        analyser::main_analyser(HOSTNAME, PORT, counters)
    );

    // Wait for the analyser to finish
    let experiment_results = analyser_task.await.unwrap();

    // Signal for the publisher tasks to stop
    *running.lock().unwrap() = false;

    // Save experiment results
    if let Err(error) = experiment::save_experiment_results(experiment_results, TOPIC_RESULTS_FILE, SYS_RESULT_FILE) {
        eprintln!("Unable to save experiment results: {}\n", error);
        return;
    } else {
        println!("Saved experiment results to the {} directory\n", EXPERIMENT_DIR);
    }

    println!("EXPERIMENTS COMPLETED");
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