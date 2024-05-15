use tokio::task;
use rumqttc::{Event, Packet, QoS};

use crate::{create_mqtt_conn, publisher_topic_string, qos_to_u8};
use crate::{INSTANCECOUNT_TOPIC, QOS_TOPIC, DELAY_TOPIC};

pub async fn main_analyser(hostname: &str, port: u16, analyser_qos: QoS, instancecount: u8, qos: QoS, delay: u64) {
    let analyser_id = "analyser";

    let (analyser, mut eventloop) = create_mqtt_conn(analyser_id, hostname, port);
    
    let publisher_topic = publisher_topic_string(instancecount, analyser_qos, delay);

    // Subscribe to the publisher topic
    if let Err(error) = analyser.subscribe(&publisher_topic, analyser_qos).await {
        eprintln!("{} failed to subscribe to publisher topic {} with error: {}", analyser_id, publisher_topic, error);
    } else {
        println!("{} subscribed to publisher topic: {}", analyser_id, publisher_topic);
    }

    task::spawn(async move {
        // Publish the instancecount
        if let Err(error) = analyser.publish(INSTANCECOUNT_TOPIC, analyser_qos, true, instancecount.to_be_bytes()).await {
            eprintln!("{} failed to publish {} to {} with error: {}", analyser_id, instancecount, INSTANCECOUNT_TOPIC, error);
        } else {
            println!("{} successfully published {} to topic: {}", analyser_id, instancecount, INSTANCECOUNT_TOPIC);
        }

        // // Publish the qos
        let qos = qos_to_u8(qos);        
        if let Err(error) = analyser.publish(QOS_TOPIC, analyser_qos, true, qos.to_be_bytes()).await {
            eprintln!("{} failed to publish {} to {} with error: {}", analyser_id, qos, QOS_TOPIC, error);
        } else {
            println!("{} successfully published {} to topic: {}", analyser_id, qos, QOS_TOPIC);
        }

        // // Publish the delay
        if let Err(error) = analyser.publish(DELAY_TOPIC, analyser_qos, true, delay.to_be_bytes()).await {
            eprintln!("{} failed to publish {} to {} with error: {}", analyser_id, delay, DELAY_TOPIC, error);
        } else {
            println!("{} successfully published {} to topic: {}", analyser_id, delay, DELAY_TOPIC);
        }
    });

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
