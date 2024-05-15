use rumqttc::{Event, Packet, QoS};

use crate::{create_mqtt_conn, publisher_topic_string};

pub async fn main_analyser(hostname: &str, port: u16, instancecount: u8, qos: QoS, delay: u64) {
    let analyser_id = "analyser";

    let (client, mut eventloop) = create_mqtt_conn(analyser_id, hostname, port);
    
    let publisher_topic = publisher_topic_string(instancecount, qos, delay);

    // Subscribe to the topic
    client.subscribe(&publisher_topic, qos).await.unwrap();

    println!("{} subscribed to publisher topic: {}", analyser_id, publisher_topic);

    // Event loop to handle incoming messages
    while let Ok(event) = eventloop.poll().await {
        match event {
            Event::Incoming(Packet::Publish(publish)) => {
                // Print the payload of the incoming message
                if publish.topic == publisher_topic {
                    println!("{} received {:?} on {}", analyser_id, publish.payload, publisher_topic);
                }
            }
            _ => {}
        }
    }
}
