mod analyser;
mod publisher;

use std::time::Duration;

use rumqttc::{AsyncClient, EventLoop, MqttOptions, QoS};

#[tokio::main]
async fn main() {
    // Broker details
    let hostname = "localhost";
    let port = 1883;

    let instancecount = 1;
    let qos = QoS::AtMostOnce;
    let delay = 1000; // ms

    println!("Spawning analyser task");

    let analyser_task = tokio::spawn(
        analyser::main_analyser(hostname, port, instancecount, qos, delay)
    );

    println!("Spawning publisher tasks");

    let mut pub_tasks = Vec::new();
    for pub_id in 1..=1 {
        pub_tasks.push(
            tokio::spawn(
                publisher::main_publisher(pub_id, hostname, port, instancecount, qos, delay)
            )
        );
    }

    println!("Waiting for tasks to finish");

    // Wait for all tasks to finish
    analyser_task.await.unwrap();
    for pub_task in pub_tasks {
        pub_task.await.unwrap();
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