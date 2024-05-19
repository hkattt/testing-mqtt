use std::sync::Arc;
use std::time::{Duration, Instant};

use rumqttc::{AsyncClient, Event, EventLoop, Packet, Publish, QoS};
use::debug_print::{debug_println, debug_eprintln};
use chrono::{Local, Timelike};

use crate::{create_mqtt_conn, publisher_topic_string, publisher_topic_instance, qos_to_u8, subscribe_to_topics};
use crate::{INSTANCECOUNT_TOPIC, QOS_TOPIC, DELAY_TOPIC, SEND_DURATION};
use crate::experiment::{ExperimentResult, TopicResult};

// Mosquitto $SYS topics
const CLIENTS_CONNECTED_TOPIC: &str  = "$SYS/broker/clients/connected";

const CURRENT_SIZE_TOPIC: &str       = "$SYS/broker/heap/current";
const MAX_SIZE_TOPIC: &str           = "$SYS/broker/heap/maximum";

const PUB_MSGS_RECEIVED_TOPIC: &str  = "$SYS/broker/publish/messages/received";
const PUB_MSGS_SENT_TOPIC: &str      = "$SYS/broker/publish/messages/sent";
const PUB_MSGS_DROPPED_TOPIC: &str   = "$SYS/broker/publish/messages/dropped";

pub async fn main_analyser(hostname: &str, port: u16) -> Vec<ExperimentResult> {
    let analyser_id = "analyser";

    // 1 to 5 publishers
    let instancecounts = [1, 2, 3, 4, 5];
    // 3 different quality-of-service levels
    let qoss = [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce];
    // 0ms, 1ms, 2ms, 4ms message delay
    let delays = [0, 1, 2, 4];

    let (analyser, mut eventloop) = create_mqtt_conn(analyser_id, hostname, port, Duration::from_secs(1));
    let analyser = Arc::new(analyser);

    let mut experiment_results = Vec::new();

    let sys_topics = vec![
        CLIENTS_CONNECTED_TOPIC, CURRENT_SIZE_TOPIC, MAX_SIZE_TOPIC, 
        PUB_MSGS_RECEIVED_TOPIC, PUB_MSGS_SENT_TOPIC, PUB_MSGS_DROPPED_TOPIC
    ];

    if subscribe_to_topics(&analyser, analyser_id, QoS::ExactlyOnce, &sys_topics).await.is_err() {
        return Vec::new();
    }

    for analyser_qos in qoss {
        for instancecount in instancecounts {
            for publisher_qos in qoss {
                for delay in delays {
                    // Get the current local time
                    let local_time = Local::now();

                    println!(
                        "\n[{:02}h:{:02}m:{:02}s]\n\
                        ANALYSER STARTING EXPERIMENT\n\
                        \tanalyser qos: {}\n\
                        \tinstancecount: {}\n\
                        \tpublisher qos: {}\n\
                        \tdelay: {}ms\n",
                        local_time.time().hour(), local_time.time().minute(), local_time.time().second(),
                        qos_to_u8(analyser_qos),
                        instancecount,
                        qos_to_u8(publisher_qos),
                        delay
                    );

                    // Conduct experiment
                    let experiment_result = 
                        conduct_experiment(&analyser, 
                                &mut eventloop, 
                                analyser_id, 
                                analyser_qos, 
                                instancecount, 
                                publisher_qos, 
                                delay)
                                .await;
                    
                    // Record experiment result
                    experiment_results.push(experiment_result);
                }
            }
        }
    }
    experiment_results
}

async fn conduct_experiment(
        analyser: &Arc<AsyncClient>, 
        eventloop: &mut EventLoop, 
        analyser_id: &str, 
        analyser_qos: QoS, 
        instancecount: u8, 
        publisher_qos: QoS, 
        delay: u64) -> ExperimentResult {
       
    // TODO: Improve this?
    let analyser_clone = Arc::clone(&analyser);
    let analyser_id_clone = analyser_id.to_string();

    // Publish the instancecount
    if let Err(error) = analyser_clone.publish(INSTANCECOUNT_TOPIC, analyser_qos, false, instancecount.to_be_bytes()).await {
        debug_eprintln!("{} failed to publish {} to {} with error: {}", analyser_id_clone, instancecount, INSTANCECOUNT_TOPIC, error);
    } else {
        debug_println!("{} successfully published {} to topic: {}", analyser_id_clone, instancecount, INSTANCECOUNT_TOPIC);
    }

    // Publish the qos
    if let Err(error) = analyser_clone.publish(QOS_TOPIC, analyser_qos, false, qos_to_u8(publisher_qos).to_be_bytes()).await {
        debug_eprintln!("{} failed to publish {} to {} with error: {}", analyser_id_clone, qos_to_u8(publisher_qos), QOS_TOPIC, error);
    } else {
        debug_println!("{} successfully published {} to topic: {}", analyser_id_clone, qos_to_u8(publisher_qos), QOS_TOPIC);
    }

    // Publish the delay
    if let Err(error) = analyser_clone.publish(DELAY_TOPIC, analyser_qos, false, delay.to_be_bytes()).await {
        debug_eprintln!("{} failed to publish {} to {} with error: {}", analyser_id_clone, delay, DELAY_TOPIC, error);
    } else {
        debug_println!("{} successfully published {} to topic: {}", analyser_id_clone, delay, DELAY_TOPIC);
    }

    let mut publisher_topics = Vec::with_capacity(instancecount as usize);
    for i in 1..=instancecount {
        let publisher_topic = publisher_topic_string(i, publisher_qos, delay);
        publisher_topics.push(publisher_topic);
    }

    if subscribe_to_topics(analyser, analyser_id, analyser_qos, &publisher_topics).await.is_err() {
        return ExperimentResult::new(
            qos_to_u8(analyser_qos),
            instancecount,
            qos_to_u8(publisher_qos),
            delay, 
            Vec::new()
        );
    }

    let mut previous_counter: [u64; 5] = [0; 5];
    let mut message_count: [u64; 5] = [0; 5];
    let mut out_of_order_count: [u64; 5] = [0; 5];
    let mut message_times: [Vec<Instant>; 5] = Default::default();

    // Mosquitto $SYS broker measurements
    let mut nconnected_clients: u64;
    let mut current_heap_size: u64;
    let mut max_heap_size: u64;
    let mut npub_msgs_recv: u64;
    let mut npub_msgs_sent: u64;
    let mut npub_msgs_dropped: u64;

    let start = std::time::Instant::now();
    // Event loop to handle incoming messages
    while start.elapsed().as_secs() <= SEND_DURATION.as_secs() {
        if let Ok(event) = eventloop.poll().await {
            match event {
                Event::Incoming(Packet::Publish(publish)) => {
                    // Print the payload of the incoming message
                    if publisher_topics.contains(&publish.topic) {
                        let i = publisher_topic_instance(&publish.topic).unwrap() - 1;
                        message_count[i] += 1;

                        let counter = bytes_to_u64(&publish);

                        if counter < previous_counter[i] {
                            out_of_order_count[i] += 1;
                        }
                        previous_counter[i] = counter;

                        message_times[i].push(std::time::Instant::now());

                        debug_println!("{} received {} on {}", analyser_id, counter, publish.topic);
                    }
                    else if publish.topic == CLIENTS_CONNECTED_TOPIC {
                        nconnected_clients = utf8_to_u64(&publish);
                        
                        debug_println!("Number of connected clients: {}", nconnected_clients);
                    }
                    else if publish.topic == CURRENT_SIZE_TOPIC {                        
                        current_heap_size = utf8_to_u64(&publish);

                        debug_println!("Current heap size: {}", current_heap_size);
                    }
                    else if publish.topic == MAX_SIZE_TOPIC {
                        max_heap_size = utf8_to_u64(&publish);
                        
                        debug_println!("Maximum heap size: {}", max_heap_size);
                    }
                    else if publish.topic == PUB_MSGS_RECEIVED_TOPIC {
                        npub_msgs_recv = utf8_to_u64(&publish);
                        
                        debug_println!("Number of publisher messages received: {}", npub_msgs_recv);
                    }
                    else if publish.topic == PUB_MSGS_SENT_TOPIC {
                        npub_msgs_sent = utf8_to_u64(&publish);

                        debug_println!("Number of publisher messages sent: {}", npub_msgs_sent);
                    }
                    else if publish.topic == PUB_MSGS_DROPPED_TOPIC {
                        npub_msgs_dropped = utf8_to_u64(&publish);

                        debug_println!("Number of publisher messages dropped: {}", npub_msgs_dropped);
                    }
                }
                _ => {}
            }
        }
    }

    let mut topic_results = Vec::new();
    
    let expected_count = compute_expected_count(delay);

    for i in 0..instancecount as usize {
        let message_rate = message_count[i] as f64 / SEND_DURATION.as_secs() as f64;
        let loss_rate = compute_loss_rate(expected_count, message_count[i]);
        let out_of_order_rate = message_count[i] as f64 / out_of_order_count[i] as f64;
        let inter_message_gap = compute_inter_message_gap(&message_times[i]);

        let topic = publisher_topic_string((i + 1) as u8, publisher_qos, delay);

        topic_results.push(
            TopicResult::new(
                topic, 
                message_rate,
                loss_rate,
                out_of_order_rate,
                inter_message_gap,
            )
        );
    }

    ExperimentResult::new(
        qos_to_u8(analyser_qos),
        instancecount,
        qos_to_u8(publisher_qos),
        delay,
        topic_results,
    )
}

fn compute_inter_message_gap(message_times: &Vec<Instant>) -> u64 {
    let len = message_times.len();

    if len <= 1 {
        return 0;
    } else if len == 2 {
        return (message_times[1] - message_times[0]).as_millis() as u64;
    }
    
    let mut time_differences = Vec::with_capacity(len - 1);

    for i in 1..len {
        time_differences.push(message_times[i] - message_times[i - 1]);
    }

    time_differences.sort();

    if len % 2 == 1 {
        time_differences[len / 2].as_millis() as u64
    } else {
        let mid1 = time_differences[len / 2 - 1].as_millis();
        let mid2 = time_differences[len / 2].as_millis();
        ((mid1 + mid2) / 2) as u64
    }
}

fn compute_loss_rate(expected_count: u64, message_count: u64) -> f64 {
    // If there is no expected count, the loss rate is zero
    if expected_count == 0 {
        return 0.0;
    }

    let lost_messages = expected_count - message_count;
    (lost_messages as f64 / expected_count as f64) * 100.0
}

fn compute_expected_count(delay: u64) -> u64 {
    // If the delay is zero, messages are theoretically sent as fast as possible
    // We estimate the expected count by setting delay = 0.5 ms
    // TODO: Idk
    // Possible idea: Pass a shared 'stat' data structure to the publishers
    if delay == 0 {
        2 * SEND_DURATION.as_millis() as u64
    } else {
        SEND_DURATION.as_millis() as u64 / delay
    }
}

fn bytes_to_u64(publish: &Publish) -> u64 {
    let payload = &publish.payload.to_vec();
    let mut array = [0u8; 8];
    let len = payload.len().min(8);
    array[..len].copy_from_slice(&payload[..len]);
    u64::from_be_bytes(array)
}

fn utf8_to_u64(publish: &Publish) -> u64 {
    let payload = &publish.payload;
    let payload_str = std::str::from_utf8(&payload).unwrap();
    payload_str.parse::<u64>().unwrap()
}