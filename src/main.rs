mod analyser;
mod publisher;
mod experiment;

use std::time::Duration;

use rumqttc::{AsyncClient, EventLoop, MqttOptions, QoS};

// Broker details
const HOSTNAME: &str            = "localhost";
const PORT: u16                 = 1883;

// MQQT Topics
const INSTANCECOUNT_TOPIC: &str = "request/instancecount";
const QOS_TOPIC: &str           = "request/qos";
const DELAY_TOPIC: &str         = "request/delay";

const RESULT_FILE: &str         = "experiment-results.csv";

// Publisher send duration (seconds)
const SEND_DURATION: u64        = 0; 
// Maximum number of publishers 
const NPUBLISHERS: u8           = 5;

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

    println!("Spawning analyser task\n");
    let analyser_task = tokio::spawn(
        analyser::main_analyser(HOSTNAME, PORT)
    );

    println!("Spawning publisher task(s)\n");
    let mut publisher_tasks = Vec::new();
    for publisher_index in 1..=NPUBLISHERS {
        publisher_tasks.push(
            tokio::spawn(
                publisher::main_publisher(publisher_index, HOSTNAME, PORT)
            )
        );
    }

    // Wait for all tasks to finish
    let experiment_results = analyser_task.await.unwrap();

    if let Err(error) = experiment::save(experiment_results) {
        eprintln!("Unable to save experiment results: {}", error);
        return;
    } else {
        println!("Saved experiment results to {}", RESULT_FILE);
    }
    // TODO: Save results after publisher tasks finish

    for publisher_task in publisher_tasks {
        publisher_task.await.unwrap();
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