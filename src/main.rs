mod analyser;
mod publisher;
mod experiment;

use std::{sync::{Arc, Mutex}, time::Duration};

use rumqttc::{AsyncClient, ClientError, EventLoop, MqttOptions, QoS};
use::debug_print::{debug_println, debug_eprintln};

// Broker details
const HOSTNAME: &str                = "localhost";
const PORT: u16                     = 1883;

// MQQT topics
const INSTANCECOUNT_TOPIC: &str     = "request/instancecount";
const QOS_TOPIC: &str               = "request/qos";
const DELAY_TOPIC: &str             = "request/delay";

// Mosquitto $SYS topics
const CURRENT_SIZE_TOPIC: &str      = "$SYS/broker/heap/current size";
const MAX_SIZE_TOPIC: &str          = "$SYS/broker/heap/maximum size";
const CONNECTIONS_TOPIC: &str       = "$SYS/broker/load/connections/1min";
const INFLIGHT_TOPIC: &str          = "$SYS/broker/messages/inflight";
const DROPPED_TOPIC: &str           = "$SYS/broker/publish/messages/dropped";
const CLIENTS_CONNECTED_TOPIC: &str = "$SYS/broker/clients/connected";

const RESULT_FILE: &str             = "experiment-results.csv";

// Publisher send duration (seconds)
const SEND_DURATION: u64            = 1; 
// Maximum number of publishers 
const NPUBLISHERS: u8               = 5;

/**
 * TODO:
 * 1. Make the Analyser start EXACTLY when the publishers finish
 *      - Just make the publisher send everything to begin with?
 * 2. Make the Publisher actually end
 * 3. The program should go VERY quickly at 0ms delay and qos = 0
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
        SEND_DURATION,
        NPUBLISHERS
    );

    let running = Arc::new(Mutex::new(true));

    println!("Spawning analyser task\n");
    let analyser_task = tokio::spawn(
        analyser::main_analyser(HOSTNAME, PORT)
    );

    println!("Spawning publisher task(s)\n");
    let mut publisher_tasks = Vec::new();
    for publisher_index in 1..=NPUBLISHERS {
        publisher_tasks.push(
            tokio::spawn(
                publisher::main_publisher(publisher_index, HOSTNAME, PORT, Arc::clone(&running))
            )
        );
    }

    // Wait for the analyser to finish
    let experiment_results = analyser_task.await.unwrap();

    // Signal for the publisher tasks to stop
    *running.lock().unwrap() = false;

    // Save experiment results
    if let Err(error) = experiment::save(experiment_results) {
        eprintln!("Unable to save experiment results: {}", error);
        return;
    } else {
        println!("Saved experiment results to {}", RESULT_FILE);
    }

    println!("EXPERIMENTS COMPLETED");
}

fn create_mqtt_conn(client_id: &str, hostname: &str, port: u16) -> (AsyncClient, EventLoop) {
    // Create MQTT options
    let mut options = MqttOptions::new(client_id, hostname, port);
    options.set_keep_alive(Duration::from_secs(5));
    options.set_clean_session(true);

    // Create MQTT client and connection 
    AsyncClient::new(options, 10)
}

async fn subscribe_to_topics(client: &AsyncClient, client_id: &str, qos: QoS, topics: &[&str]) -> Result<(), ClientError>{
    for topic in topics {
        if let Err(error) = client.subscribe(*topic, qos).await {
            // TODO: Replace with debug prints
            debug_eprintln!("{} failed to subscribe to topic {} with error: {}", client_id, topic, error);
            return Err(error);
        } else {
            // TODO: Replace with debug prints
            debug_println!("{} subscribed to topic: {}", client_id, topic);
        }
    }
    Ok(())
}

fn publisher_topic_string(instancecount: u8, qos: QoS, delay: u64) -> String {
    format!("counter/{}/{}/{}", instancecount, qos_to_u8(qos), delay)
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