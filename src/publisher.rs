use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::task;
use rumqttc::{AsyncClient, Event, EventLoop, Packet, QoS};
use::debug_print::{debug_println, debug_eprintln};

use crate::{bytes_to_u64, create_mqtt_conn, publisher_topic_string, qos_to_u8, subscribe_to_topics, u8_to_qos};
use crate::{INSTANCECOUNT_TOPIC, QOS_TOPIC, DELAY_TOPIC, SEND_DURATION};

pub async fn main_publisher(publisher_index: u8, hostname: &str, port: u16, running: Arc<Mutex<bool>>, counter: Arc<Mutex<u64>>) {
    let publisher_id = format!("pub-{}", publisher_index);

    let (publisher, mut eventloop) = create_mqtt_conn(&publisher_id, hostname, port, Duration::from_secs(5));
    
    println!("{} connected to {}:{}", publisher_id, hostname, port);
    
    let publisher = Arc::new(publisher);

    // Subscribe to instancecount, qos, and delay topics
    // Use the highest level of QoS to ensure delivery. Need to receive the experiment parameters
    let topics = [INSTANCECOUNT_TOPIC, QOS_TOPIC, DELAY_TOPIC];
    if subscribe_to_topics(&publisher, &publisher_id, QoS::ExactlyOnce, &topics).await.is_err() {
        return;
    }

    // Publishers keep running until they are signaled to stop
    // This will occur when the analyser finishes
    while *running.lock().unwrap() {
        // Receive instancecount, qos, and delay from the analyser
        let (instancecount, qos, delay) = match receive_topic_values(&mut eventloop, &publisher_id).await {
            Some((instancecount, qos, delay)) => (instancecount, qos, delay),
            None => return
        };

        let publisher_topic = publisher_topic_string(publisher_index, qos, delay);

        // TODO: Surely we can do this some other way?
        let publisher_clone = Arc::clone(&publisher);
        let counter_clone = Arc::clone(&counter);
        let publisher_id_clone = publisher_id.clone();

        let counter_task = task::spawn(async move {
            if publisher_index <= instancecount {
                println!("{} publishing to {} using instancecount: {}, qos: {:?}, and delay: {}", 
                    publisher_id_clone, publisher_topic, instancecount, qos_to_u8(qos), delay);
                *counter_clone.lock().unwrap() = publish_counter(&publisher_clone, &publisher_id_clone, &publisher_topic, qos, delay).await;
            }
        });

        while !counter_task.is_finished() {
            eventloop.poll().await.unwrap();
        }
    }
}

async fn publish_counter(publisher: &AsyncClient, publisher_id: &str, publisher_topic: &str, qos: QoS, delay: u64) -> u64 {
    let mut counter: u64 = 0;
    let start = std::time::Instant::now();
    let delay = Duration::from_millis(delay);
    while start.elapsed().as_secs() < SEND_DURATION.as_secs() {
        // Publish the counter value
        if let Err(error) = publisher.publish(publisher_topic, qos, false, counter.to_be_bytes()).await {
            debug_eprintln!("{} failed to publish {} to {} with error: {}", publisher_id, counter, publisher_topic, error);
        } else {
            debug_println!("{} successfully published {} to topic: {}", publisher_id, counter, publisher_topic);
        }

        counter += 1;

        tokio::time::sleep(delay).await;
    }
    counter
} 

async fn receive_topic_values(eventloop: &mut EventLoop, publisher_id: &str) -> Option<(u8, QoS, u64)> {
    let mut instancecount = None;
    let mut qos = None;
    let mut delay = None;

    // Event loop to handle incoming messages
    loop {
        if instancecount.is_some() && qos.is_some() && delay.is_some() {
            break;
        }

        if let Ok(event) = eventloop.poll().await {
            match event {
                Event::Incoming(Packet::Publish(publish)) => {
                    // Receive instancecount 
                    if publish.topic == INSTANCECOUNT_TOPIC {
                        instancecount = Some(u8::from_be_bytes([publish.payload[0]]));
                    } // Receive qos 
                    else if publish.topic == QOS_TOPIC {
                        qos = Some(
                            u8_to_qos(
                                u8::from_be_bytes([publish.payload[0]])
                            )
                        );
                    } // Receive delay 
                    else if publish.topic == DELAY_TOPIC {
                        delay = Some(bytes_to_u64(&publish));
                    } 
                }
                _ => ()
            }
        }
    }
    // At this point, it is safe to unwrap the variables

    let qos = match qos.unwrap() {
        Some(qos) => qos,
        None => {
            debug_eprintln!("{} received invalid qos", publisher_id);
            return None;
        }
    };
    Some((instancecount.unwrap(), qos, delay.unwrap()))
}