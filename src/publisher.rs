use std::sync::Arc;

use tokio::task;
use rumqttc::{AsyncClient, ClientError, Event, EventLoop, Packet, QoS};
use::debug_print::{debug_println, debug_eprintln};

use crate::{create_mqtt_conn, publisher_topic_string, u8_to_qos, qos_to_u8};
use crate::{INSTANCECOUNT_TOPIC, QOS_TOPIC, DELAY_TOPIC, SEND_DURATION};

pub async fn main_publisher(publisher_index: u8, hostname: &str, port: u16) {
    let publisher_id = format!("pub-{}", publisher_index);

    let (publisher, mut eventloop) = create_mqtt_conn(&publisher_id, hostname, port);
    let publisher = Arc::new(publisher);

    // Subscribe to instancecount, qos, and delay topics
    // Use the highest level of QoS to ensure delivery. Need to receive the experiment parameters
    subscribe_to_topics(&publisher, &publisher_id, QoS::ExactlyOnce).await.unwrap_or_else(|_| return);

    loop {
        // Receive instancecount, qos, and delay from the analyser
        let (instancecount, qos, delay) = match receive_topic_values(&mut eventloop, &publisher_id).await {
            Some((instancecount, qos, delay)) => (instancecount, qos, delay),
            None => return
        };

        let publisher_topic = publisher_topic_string(instancecount, qos, delay);

        // TODO: Surely we can do this some other way?
        let publisher_clone = Arc::clone(&publisher);
        let publisher_id_clone = publisher_id.clone();

        let counter_task = task::spawn(async move {
            if publisher_index <= instancecount {
                println!("{} publishing to {} using instancecount: {}, qos: {:?}, and delay: {}", 
                    publisher_id_clone, publisher_topic, instancecount, qos_to_u8(qos), delay);
                publish_counter(&publisher_clone, &publisher_id_clone, &publisher_topic, qos, delay).await;
            }
        });

        while !counter_task.is_finished() {
            eventloop.poll().await.unwrap();
        }
    }
}

async fn publish_counter(publisher: &AsyncClient, publisher_id: &str, publisher_topic: &str, qos: QoS, delay: u64) {
    let start = std::time::Instant::now();

    for counter in 0.. {
        // Publish the counter value
        if let Err(error) = publisher.publish(publisher_topic, qos, false, counter.to_string()).await {
            debug_eprintln!("{} failed to publish {} to {} with error: {}", publisher_id, counter, publisher_topic, error);
        } else {
            debug_println!("{} successfully published {} to topic: {}", publisher_id, counter, publisher_topic);
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;

        if start.elapsed().as_secs() > SEND_DURATION {
            break;
        } 
    }
} 

async fn subscribe_to_topics(publisher: &AsyncClient, publisher_id: &str, qos: QoS) -> Result<(), ClientError> {
    // Subscribe to the instancecount topic
    if let Err(error) = publisher.subscribe(INSTANCECOUNT_TOPIC, qos).await {
        debug_eprintln!("{} failed to subscribe to topic {} with error: {}", publisher_id, INSTANCECOUNT_TOPIC, error);
        return Err(error);
    } else {
        debug_println!("{} subscribed to topic: {}", publisher_id, INSTANCECOUNT_TOPIC);
    }

    // Subscribe to the instancecount topic
    if let Err(error) = publisher.subscribe(QOS_TOPIC, qos).await {
        debug_eprintln!("{} failed to subscribe to topic {} with error: {}", publisher_id, QOS_TOPIC, error);
        return Err(error);
    } else {
        debug_println!("{} subscribed to topic: {}", publisher_id, QOS_TOPIC);
    }

    // Subscribe to the instancecount topic
    if let Err(error) = publisher.subscribe(DELAY_TOPIC, qos).await {
        debug_eprintln!("{} failed to subscribe to topic {} with error: {}", publisher_id, DELAY_TOPIC, error);
        return Err(error);
    } else {
        debug_println!("{} subscribed to topic: {}", publisher_id, DELAY_TOPIC);
    }
    Ok(())
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
                        debug_println!("{} received {:?} on {}", publisher_id, instancecount, INSTANCECOUNT_TOPIC);
                    } // Receive qos 
                    else if publish.topic == QOS_TOPIC {
                        qos = Some(
                            u8_to_qos(
                                u8::from_be_bytes([publish.payload[0]])
                            )
                        );
                        debug_println!("{} received {:?} on {}", publisher_id, qos, QOS_TOPIC);
                    } // Receive delay 
                    else if publish.topic == DELAY_TOPIC {
                        if let Ok(bytes_array) = publish.payload[0..=7].try_into() {
                            delay = Some(u64::from_be_bytes(bytes_array));
                        } else {
                            debug_eprintln!("{} unable to convert {:?} to u64 delay on {}", publisher_id, publish.payload, DELAY_TOPIC);
                            return None;
                        }
                        debug_println!("{} received {:?} on {}", publisher_id, delay, DELAY_TOPIC);
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
    Some(
        (instancecount.unwrap(), qos, delay.unwrap())
    )
}