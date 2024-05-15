use tokio::task;
use rumqttc::{AsyncClient, ClientError, QoS, Event, Packet};

use crate::{create_mqtt_conn, publisher_topic_string, u8_to_qos};
use crate::{INSTANCECOUNT_TOPIC, QOS_TOPIC, DELAY_TOPIC, SEND_DURATION};

pub async fn main_publisher(publisher_id: u16, hostname: &str, port: u16) {
    let publisher_id = format!("publisher{}", publisher_id);

    let (publisher, mut eventloop) = create_mqtt_conn(&publisher_id, hostname, port);

    // Subscribe to instancecount, qos, and delay topics
    // TODO: What QoS should we use here?
    subscribe_to_topics(&publisher, &publisher_id, QoS::AtLeastOnce).await.unwrap_or_else(|_| return);

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
                        println!("{} received {:?} on {}", publisher_id, instancecount, INSTANCECOUNT_TOPIC);
                    } // Receive qos 
                    else if publish.topic == QOS_TOPIC {
                        qos = Some(
                            u8_to_qos(
                                u8::from_be_bytes([publish.payload[0]])
                            )
                        );
                        println!("{} received {:?} on {}", publisher_id, qos, QOS_TOPIC);
                    } // Receive delay 
                    else if publish.topic == DELAY_TOPIC {
                        if let Ok(bytes_array) = publish.payload[0..=7].try_into() {
                            delay = Some(u64::from_be_bytes(bytes_array));
                        } else {
                            eprintln!("{} unable to convert {:?} to u64 delay on {}", publisher_id, publish.payload, DELAY_TOPIC);
                            return;
                        }
                        println!("{} received {:?} on {}", publisher_id, delay, DELAY_TOPIC);
                    } 
                }
                _ => ()
            }
        }
    }

    let instancecount = instancecount.unwrap();
    let qos = qos.unwrap().unwrap(); // TODO: handle this 
    let delay = delay.unwrap();

    println!("{} using instancecount: {}, qos: {:?}, and delay: {}", publisher_id, instancecount, qos, delay);

    let publisher_topic = publisher_topic_string(instancecount, qos, delay);

    let counter_task = task::spawn(async move {
        let start = std::time::Instant::now();

        for counter in 0.. {
            // Publish the counter value
            if let Err(error) = publisher.publish(&publisher_topic, qos, false, counter.to_string()).await {
                eprintln!("{} failed to publish {} to {} with error: {}", publisher_id, counter, publisher_topic, error);
            } else {
                println!("{} successfully published {} to topic: {}", publisher_id, counter, publisher_topic);
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;

            if start.elapsed().as_secs() > SEND_DURATION {
                break;
            } 
        }
    });

    while !counter_task.is_finished() {
        eventloop.poll().await.unwrap();
    }
}

async fn subscribe_to_topics(publisher: &AsyncClient, publisher_id: &str, qos: QoS) -> Result<(), ClientError> {
    // Subscribe to the instancecount topic
    if let Err(error) = publisher.subscribe(INSTANCECOUNT_TOPIC, qos).await {
        eprintln!("{} failed to subscribe to topic {} with error: {}", publisher_id, INSTANCECOUNT_TOPIC, error);
        return Err(error);
    } else {
        println!("{} subscribed to topic: {}", publisher_id, INSTANCECOUNT_TOPIC);
    }

    // Subscribe to the instancecount topic
    if let Err(error) = publisher.subscribe(QOS_TOPIC, qos).await {
        eprintln!("{} failed to subscribe to topic {} with error: {}", publisher_id, QOS_TOPIC, error);
        return Err(error);
    } else {
        println!("{} subscribed to topic: {}", publisher_id, QOS_TOPIC);
    }

    // Subscribe to the instancecount topic
    if let Err(error) = publisher.subscribe(DELAY_TOPIC, qos).await {
        eprintln!("{} failed to subscribe to topic {} with error: {}", publisher_id, DELAY_TOPIC, error);
        return Err(error);
    } else {
        println!("{} subscribed to topic: {}", publisher_id, DELAY_TOPIC);
    }
    Ok(())
}