mod analyser;
mod publisher;

use std::time::Duration;

use rumqttc::{AsyncClient, EventLoop, MqttOptions, QoS};

const INSTANCECOUNT_TOPIC: &str = "request/instancecount";
const QOS_TOPIC: &str = "request/qos";
const DELAY_TOPIC: &str = "request/delay";
const SEND_DURATION: u64 = 5; // Seconds

#[tokio::main]
async fn main() {
    // Broker details
    let hostname = "localhost";
    let port = 1883;

    let instancecount = 1;
    let qos = QoS::AtMostOnce;
    let delay = 1000; // ms

    let analyser_qos = QoS::AtLeastOnce;
    let npublishers = 1;

    println!("SPAWNING ANALYSER TASK\n");

    let analyser_task = tokio::spawn(
        analyser::main_analyser(hostname, port, analyser_qos, instancecount, qos, delay)
    );

    println!("SPAWNING {} PUBLISHER TASK(S)\n", npublishers);

    let mut publisher_tasks = Vec::new();
    for publisher_id in 1..=npublishers {
        publisher_tasks.push(
            tokio::spawn(
                publisher::main_publisher(publisher_id, hostname, port)
            )
        );
    }

    println!("WAITING FOR TASKS TO FINISH\n");

    // Wait for all tasks to finish
    analyser_task.await.unwrap();
    for publisher_task in publisher_tasks {
        publisher_task.await.unwrap();
    }
}

fn create_mqtt_conn(client_id: &str, hostname: &str, port: u16) -> (AsyncClient, EventLoop) {
    // Create MQTT options
    let mut options = MqttOptions::new(client_id, hostname, port);
    options.set_keep_alive(Duration::from_secs(5));

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