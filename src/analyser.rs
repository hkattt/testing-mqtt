use std::sync::Arc;

use tokio::task;
use rumqttc::{AsyncClient, EventLoop, Event, Packet, QoS};
use::debug_print::{debug_println, debug_eprintln};

use crate::{create_mqtt_conn, publisher_topic_string, qos_to_u8, SEND_DURATION};
use crate::{INSTANCECOUNT_TOPIC, QOS_TOPIC, DELAY_TOPIC};

pub async fn main_analyser(hostname: &str, port: u16, analyser_qos: QoS) {
    let analyser_id = "analyser";

    // 1 to 5 publishers
    let instancecounts = [1, 2, 3, 4, 5];
    // 3 different quality-of-service levels
    let qoss = [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce];
    // 0ms, 1ms, 2ms, 4ms message delay
    let delays = [0, 1, 2, 4];

    let (analyser, mut eventloop) = create_mqtt_conn(analyser_id, hostname, port);
    let analyser = Arc::new(analyser);

    for instancecount in instancecounts {
        for qos in qoss {
            for delay in delays {
                println!(
                    "\nANALYSER STARTING COMBINATION
                    instancecount: {}
                    qos: {}
                    delay: {}ms\n",
                    instancecount,
                    qos_to_u8(qos),
                    delay
                );
                analyse_combination(&analyser, &mut eventloop, analyser_id, analyser_qos, instancecount, qos, delay).await;
            }
        }
    }
}

async fn analyse_combination(analyser: &Arc<AsyncClient>, eventloop: &mut EventLoop, analyser_id: &str, analyser_qos: QoS, instancecount: u8, qos: QoS, delay: u64) {
    
    let publisher_topic = publisher_topic_string(instancecount, qos, delay);

    // Subscribe to the publisher topic
    if let Err(error) = analyser.subscribe(&publisher_topic, analyser_qos).await {
        debug_eprintln!("{} failed to subscribe to publisher topic {} with error: {}", analyser_id, publisher_topic, error);
    } else {
        debug_println!("{} subscribed to publisher topic: {}", analyser_id, publisher_topic);
    }

    // TODO: Improve this?
    let analyser_clone = Arc::clone(&analyser);
    let analyser_id_clone = analyser_id.to_string();

    task::spawn(async move {
        // Publish the instancecount
        if let Err(error) = analyser_clone.publish(INSTANCECOUNT_TOPIC, analyser_qos, false, instancecount.to_be_bytes()).await {
            debug_eprintln!("{} failed to publish {} to {} with error: {}", analyser_id_clone, instancecount, INSTANCECOUNT_TOPIC, error);
        } else {
            debug_println!("{} successfully published {} to topic: {}", analyser_id_clone, instancecount, INSTANCECOUNT_TOPIC);
        }

        // // Publish the qos
        let qos = qos_to_u8(qos);        
        if let Err(error) = analyser_clone.publish(QOS_TOPIC, analyser_qos, false, qos.to_be_bytes()).await {
            debug_eprintln!("{} failed to publish {} to {} with error: {}", analyser_id_clone, qos, QOS_TOPIC, error);
        } else {
            debug_println!("{} successfully published {} to topic: {}", analyser_id_clone, qos, QOS_TOPIC);
        }

        // // Publish the delay
        if let Err(error) = analyser_clone.publish(DELAY_TOPIC, analyser_qos, false, delay.to_be_bytes()).await {
            debug_eprintln!("{} failed to publish {} to {} with error: {}", analyser_id_clone, delay, DELAY_TOPIC, error);
        } else {
            debug_println!("{} successfully published {} to topic: {}", analyser_id_clone, delay, DELAY_TOPIC);
        }
    });

    let start = std::time::Instant::now();

    // Event loop to handle incoming messages
    while let Ok(event) = eventloop.poll().await {
        match event {
            Event::Incoming(Packet::Publish(publish)) => {
                // Print the payload of the incoming message
                if publish.topic == publisher_topic {
                    debug_println!("{} received {:?} on {}", analyser_id, publish.payload, publisher_topic);
                }
            }
            _ => {}
        }
        if start.elapsed().as_secs() > SEND_DURATION {
            break;
        }
    }
}