use rumqttc::{
    AsyncClient, 
    Event, 
    EventLoop, 
    Packet, 
    QoS
};
use tokio::task;
use debug_print::{debug_println, debug_eprintln};

use std::{
    sync::{Arc, Mutex},
    time::Duration
};

use crate::{
    mqqt_helper,
    INSTANCECOUNT_TOPIC,
    QOS_TOPIC,
    DELAY_TOPIC,
    SEND_DURATION
};

/// The main publisher function. 
/// 
/// # Arguments
/// * `publisher_index`: Publisher instance
/// * `hostname`: Host name of the MQQT broker
/// * `port`: Port that the MQQT broker is on
/// * `running`: Indicates if the analyser is still running
/// * `counter`: Counter publishes by the publisher
/// 
/// The publisher connects to the provided MQQT broker hostname and port before
/// subscribing to the instancecount, qos and delay topics. The publisher continuously 
/// queries these topics for the next experiment parameters. Publishes a counter using
/// the provided parameters for SEND_DURATION seconds. Stops once the analyser is complete.
pub async fn main_publisher(
        publisher_index: u8, 
        hostname: Arc<String>, 
        port: u16, 
        running: Arc<Mutex<bool>>, 
        counter: Arc<Mutex<u64>>) 
    {
    let publisher_id = format!("pub-{}", publisher_index);

    let (publisher, mut eventloop) = mqqt_helper::create_mqtt_conn(
        &publisher_id, &hostname, port, Duration::from_secs(5)
    );
    
    println!("{} connected to {}:{}", publisher_id, hostname, port);
    
    let publisher = Arc::new(publisher);

    // Subscribe to instancecount, qos, and delay topics
    // Use the highest level of QoS to ensure delivery. Need to receive the experiment parameters
    let topics = [INSTANCECOUNT_TOPIC, QOS_TOPIC, DELAY_TOPIC];
    if mqqt_helper::subscribe_to_topics(&publisher, &publisher_id, QoS::ExactlyOnce, &topics).await.is_err() {
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

        let publisher_topic = mqqt_helper::publisher_topic_string(publisher_index, qos, delay);

        // Clone variables to pass to the task thread
        let publisher_clone = Arc::clone(&publisher);
        let counter_clone = Arc::clone(&counter);
        let publisher_id_clone = publisher_id.clone();

        // Start the counter task
        let counter_task = task::spawn(async move {
            if publisher_index <= instancecount {
                debug_println!("{} publishing to {} using instancecount: {}, qos: {:?}, and delay: {}", 
                    publisher_id_clone, publisher_topic, instancecount, mqqt_helper::qos_to_u8(qos), delay);
                *counter_clone.lock().unwrap() = publish_counter(&publisher_clone, &publisher_id_clone, &publisher_topic, qos, delay).await;
            }
        });

        // Poll the eventloop to keep the MQQT connection alive
        while !counter_task.is_finished() {
            eventloop.poll().await.unwrap();
        }
    }
}

/// Publishes a counter for the SEND_DURATION seconds. 
/// 
/// # Arguments
/// * `publisher`: The publisher to publish the counter
/// * `publisher_id`: Unique ID of the publisher
/// * `publisher_topic`: Topic to publish the counter to
/// * `qos`: Quality-of-service to send the counter with
/// * `delay`: Delay between each counter message
/// 
/// # Returns
/// The final counter value
async fn publish_counter(publisher: &AsyncClient, _publisher_id: &str, publisher_topic: &str, qos: QoS, delay: u64) -> u64 {
    let mut counter: u64 = 0;
    let delay = Duration::from_millis(delay);

    // Start the timer
    let start = std::time::Instant::now();

    // Stop sending once SEND_DURATION seconds has passed
    while start.elapsed().as_secs() < SEND_DURATION.as_secs() {
        // Publish the counter value
        if let Err(_error) = publisher.publish(publisher_topic, qos, false, counter.to_be_bytes()).await {
            debug_eprintln!("{} failed to publish {} to {} with error: {}", _publisher_id, counter, publisher_topic, _error);
        } else {
            debug_println!("{} successfully published {} to topic: {}", _publisher_id, counter, publisher_topic);
        }

        counter += 1;

        tokio::time::sleep(delay).await;
    }
    counter
} 

/// Receive the topic values (i.e. experiment parameters) from the analyser
/// 
/// # Arguments
/// * `eventloop`: The eventloop that the publisher is connected to the analyser on
/// * `publisher_id`: Unique ID of the publisher
/// 
/// # Returns
/// The (instancecount, qos, delay) if received successfully. Returns None if an invalid QoS 
/// is received
async fn receive_topic_values(eventloop: &mut EventLoop, _publisher_id: &str) -> Option<(u8, QoS, u64)> {
    let mut instancecount = None;
    let mut qos = None;
    let mut delay = None;

    loop {
        // Stop looping  once everything variable has been received
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
                            mqqt_helper::u8_to_qos(
                                u8::from_be_bytes([publish.payload[0]])
                            )
                        );
                    } // Receive delay 
                    else if publish.topic == DELAY_TOPIC {
                        delay = Some(mqqt_helper::bytes_to_u64(&publish));
                    } 
                }
                _ => ()
            }
        }
    }
    // At this point, we know that it is safe to unwrap the variables
    let qos = match qos.unwrap() {
        Some(qos) => qos,
        None => {
            debug_eprintln!("{} received invalid qos", _publisher_id);
            return None;
        }
    };
    Some((instancecount.unwrap(), qos, delay.unwrap()))
}