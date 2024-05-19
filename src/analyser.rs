use std::sync::Arc;
use std::time::{Duration, Instant};

use rumqttc::{AsyncClient, EventLoop, Event, Packet, QoS};
use::debug_print::{debug_println, debug_eprintln};
use chrono::{Local, Timelike};

use crate::{create_mqtt_conn, publisher_topic_string, publisher_topic_instance, qos_to_u8, subscribe_to_topics};
use crate::{INSTANCECOUNT_TOPIC, QOS_TOPIC, DELAY_TOPIC, CURRENT_SIZE_TOPIC, CONNECTIONS_TOPIC, MAX_SIZE_TOPIC, INFLIGHT_TOPIC, DROPPED_TOPIC, CLIENTS_CONNECTED_TOPIC, SEND_DURATION};
use crate::experiment::{ExperimentResult, TopicResult};

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
    
    let publisher_topic = publisher_topic_string(instancecount, publisher_qos, delay);
    
    let topics = [&publisher_topic, CURRENT_SIZE_TOPIC, MAX_SIZE_TOPIC, CONNECTIONS_TOPIC, INFLIGHT_TOPIC, DROPPED_TOPIC, CLIENTS_CONNECTED_TOPIC];
    
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

    if subscribe_to_topics(analyser, analyser_id, analyser_qos, &topics).await.is_err() {
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

    // $SYS Measurements
    // let average_heap_size: u64 = 0;
    // let max_heap_size: u64 = 0;
    // let connections: u64 = 0;

    let start = std::time::Instant::now();
    // Event loop to handle incoming messages
    while start.elapsed().as_secs() <= SEND_DURATION.as_secs() {
        if let Ok(event) = eventloop.poll().await {
            match event {
                Event::Incoming(Packet::Publish(publish)) => {
                    // Print the payload of the incoming message
                    if publish.topic == publisher_topic {
                        let i = publisher_topic_instance(&publisher_topic).unwrap() - 1;
                        message_count[i] += 1;

                        let mut array = [0u8; 8];

                        let len = publish.payload.len().min(8);
                        array[..len].copy_from_slice(&publish.payload[..len]);

                        let counter = u64::from_be_bytes(array);
                        if counter < previous_counter[i] {
                            out_of_order_count[i] += 1;
                        }
                        previous_counter[i] = counter;

                        message_times[i].push(std::time::Instant::now());

                        debug_println!("{} received {:?} on {}", analyser_id, publish.payload, publisher_topic);
                    }
                    else if publish.topic == CURRENT_SIZE_TOPIC {
                        // TODO: Make debug print
                        debug_println!("Current heap size: {:?}", publish.payload);
                    }
                    else if publish.topic == MAX_SIZE_TOPIC {
                        // TODO: Make debug print
                        debug_println!("Maximum heap size: {:?}", publish.payload);
                    }
                    else if publish.topic == CONNECTIONS_TOPIC {
                        // TODO: Make debug print
                        debug_println!("Current heap size: {:?}", publish.payload);
                    }
                    else if publish.topic == INFLIGHT_TOPIC {
                        // TODO: Make debug print
                        debug_println!("Number of inflight messages: {:?}", publish.payload);
                    }
                    else if publish.topic == DROPPED_TOPIC {
                        // TODO: Make debug print
                        debug_println!("Number of dropped messages: {:?}", publish.payload);
                    }
                    else if publish.topic == CLIENTS_CONNECTED_TOPIC {
                        // TODO: Make debug print
                        debug_println!("Number of clients messages: {:?}", publish.payload);
                    }
                }
                _ => {}
            }
        }
    }

    let mut topic_results = Vec::new();
    
    let expected_count = SEND_DURATION.as_millis() as u64 / (delay + 1);

    for i in 0..instancecount as usize {
        let message_rate = message_count[i] as f64 / SEND_DURATION.as_secs() as f64;
        let loss_rate = expected_count as f64 / message_count[i] as f64;
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

    if len == 0 {
        return 0;
    } else if len == 1 {
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