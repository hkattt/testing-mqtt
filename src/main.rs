use std::time::{Duration, SystemTime};

use rumqttc::{AsyncClient, ClientError, Event, EventLoop, Incoming, MqttOptions, QoS};
use std::thread;

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
        main_analyser(hostname, port, instancecount, qos, delay)
    );

    println!("Spawning publisher tasks");

    let mut pub_tasks = Vec::new();
    for pub_id in 1..=1 {
        pub_tasks.push(
            tokio::spawn(
                main_publisher(pub_id, hostname, port, instancecount, qos, delay)
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

async fn main_analyser(hostname: &str, port: u16, instancecount: u8, qos: QoS, delay: u64) {
    let analyser_id = "analyser";

    let (analyser, mut eventloop) = create_mqtt_conn(analyser_id, hostname, port);

    println!("Successfull MQTT connection: {}", analyser_id);

    let topic = publisher_topic_string(instancecount, qos, delay);

    if let Err(error) = analyser.subscribe(&topic, qos).await {
        eprintln!("Analyser unable to subscribe to topic {} with error: {}", topic, error);
        return;
    }

    println!("Analyser successfully subscribed to topic {}", topic);

    while let Ok(event) = eventloop.poll().await {
        match event {
            Event::Incoming(packet) => {
                if let rumqttc::Packet::Publish(publish) = packet {
                    if publish.topic == topic {
                        println!("Received message: {:?} on topic {}", publish.payload, topic);
                    }
                }
            }
            _ => {}
        }
    }
}

async fn main_publisher(pub_id: u16, hostname: &str, port: u16, instancecount: u8, qos: QoS, delay: u64) {
    let pub_id = format!("client{}", pub_id);
    // TODO: Create unique ID
    let (publisher, _eventloop) = create_mqtt_conn(&pub_id, hostname, port);

    println!("Successfull MQTT connection: {}", pub_id);

    let topic = publisher_topic_string(instancecount, qos, delay);

    if let Err(error) = publisher.subscribe(&topic, qos).await {
        eprintln!("Publisher unable to subscribe to topic {} with error: {}", topic, error);
        return;
    }

    println!("Publisher successfully subscribed to topic {}", topic);

    if let Err(error) = publish_counter(publisher, &topic, qos, delay).await {
        eprintln!("Error occured publishing counter: {}", error);
        return;
    }
}

fn create_mqtt_conn(client_id: &str, hostname: &str, port: u16) -> (AsyncClient, EventLoop) {
    // Create MQTT options
    let mut options = MqttOptions::new(client_id, hostname, port);
    options.set_keep_alive(Duration::from_secs(5));

    // Create MQTT client and connection 
    AsyncClient::new(options, 10)
}

async fn publish_counter(client: AsyncClient, topic: &str, qos: QoS, delay: u64) -> Result<(), ClientError>{
    let mut counter = 0;
    let start_time = SystemTime::now();

    while start_time.elapsed().unwrap().as_secs() < 60 {
        client.publish(topic, qos, false, counter.to_string()).await?;
        println!("Publisher successfully published {} to topic {}", counter, topic);
        counter += 1;
        thread::sleep(Duration::from_millis(delay));
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