use rumqttc::{
    AsyncClient, 
    Event, 
    EventLoop, 
    Packet, 
    QoS
};
use debug_print::{debug_println, debug_eprintln};
use chrono::{Local, Timelike};

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{
    mqqt_helper,
    experiment::{
        ExperimentResult,
        SysResult,
        TopicResult
    },
    INSTANCECOUNT_TOPIC,
    QOS_TOPIC,
    DELAY_TOPIC,
    SEND_DURATION
};

// Mosquitto $SYS topics
const CLIENTS_CONNECTED_TOPIC: &str  = "$SYS/broker/clients/connected";
const CURRENT_SIZE_TOPIC: &str       = "$SYS/broker/heap/current";
const MAX_SIZE_TOPIC: &str           = "$SYS/broker/heap/maximum";
const PUB_MSGS_RECEIVED_TOPIC: &str  = "$SYS/broker/publish/messages/received";
const PUB_MSGS_SENT_TOPIC: &str      = "$SYS/broker/publish/messages/sent";
const PUB_MSGS_DROPPED_TOPIC: &str   = "$SYS/broker/publish/messages/dropped";

/// The main analyser function. 
/// 
/// The analyser connects to the provided MQQT broker hostname and port before
/// iterating over all of the experiment parameters. Sends the experiment parameters
/// to the publishers and takes measurements of the network performance.
/// 
/// # Arguments
/// * `hostname`: Host name of the MQQT broker
/// * `port`: Port that the MQQT broker is on
/// * `instancecounts`: Instance count to use for the experiments
/// * `qoss`: Quality-of-service options to use for the experiments
/// * `delays`: Publisher message delays to use for the experiments
/// * `counters`: Counter values updated by the publishers
/// 
/// # Returns
/// The experiment results from the experiment
pub async fn main_analyser(
        hostname: Arc<String>, 
        port: u16,
        instancecounts: Vec<u8>,
        qoss: Vec<QoS>,
        delays: Vec<u64>, 
        counters: Vec<Arc<Mutex<u64>>>) -> Vec<ExperimentResult> 
    {
    let analyser_id = "analyser";
    
    // Connects to the MQQT broker
    let (analyser, mut eventloop) = mqqt_helper::create_mqtt_conn(
        analyser_id, &hostname, port, Duration::from_secs(1)
    );
    
    println!("{} connected to {}:{}", analyser_id, hostname, port);
    
    let sys_topics = vec![
        CLIENTS_CONNECTED_TOPIC, CURRENT_SIZE_TOPIC, MAX_SIZE_TOPIC, 
        PUB_MSGS_RECEIVED_TOPIC, PUB_MSGS_SENT_TOPIC, PUB_MSGS_DROPPED_TOPIC
    ];
    // Subscribes to the broker $SYS topics
    if mqqt_helper::subscribe_to_topics(&analyser, analyser_id, QoS::ExactlyOnce, &sys_topics).await.is_err() {
        return Vec::new();
    }

    let mut experiment_results = Vec::new();

    // Iterates over every combination of the experiment parameters
    for analyser_qos in &qoss {
        for instancecount in &instancecounts {
            for publisher_qos in &qoss {
                for delay in &delays {
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
                        mqqt_helper::qos_to_u8(*analyser_qos),
                        instancecount,
                        mqqt_helper::qos_to_u8(*publisher_qos),
                        delay
                    );
                    // Conduct experiment
                    let experiment_result = 
                        conduct_experiment(&analyser, 
                                &mut eventloop, 
                                analyser_id, 
                                *analyser_qos, 
                                *instancecount, 
                                *publisher_qos, 
                                *delay,
                                &counters).await; 
                    // Record experiment result
                    experiment_results.push(experiment_result);
                }
            }
        }
    }
    experiment_results
}

/// Conducts and analyses a single experiment with the given parameters.
/// 
/// # Arguments
/// * `analyser`: The analyser conducting the experiment
/// * `eventloop`: The eventloop that the analyser is connected to 
/// * `analyser_id`: Unique ID of the analyser
/// * `analyser_qos`: The quality-of-service of the analyser for the experiment
/// * `instancecount`: The instance count for the experiment
/// * `publisher_qos`: The quality-of-service of the publisher for the experiment
/// * `delay`: The delay of the publisher for the experiment
/// * `counter`: Counter values updated by the publishers
/// 
/// # Returns
/// The experiment result of the single experiment
async fn conduct_experiment(
        analyser: &AsyncClient, 
        eventloop: &mut EventLoop, 
        analyser_id: &str, 
        analyser_qos: QoS, 
        instancecount: u8, 
        publisher_qos: QoS, 
        delay: u64,
        counters: &Vec<Arc<Mutex<u64>>>) -> ExperimentResult 
    {
    // Publish the instancecount
    if let Err(_error) = analyser.publish(
        INSTANCECOUNT_TOPIC, QoS::ExactlyOnce, false, instancecount.to_be_bytes()
    ).await {
        debug_eprintln!("{} failed to publish {} to {} with error: {}", analyser_id, instancecount, INSTANCECOUNT_TOPIC, _error);
    } else {
        debug_println!("{} successfully published {} to topic: {}", analyser_id, instancecount, INSTANCECOUNT_TOPIC);
    }
    // Publish the qos
    if let Err(_error) = analyser.publish(
        QOS_TOPIC, QoS::ExactlyOnce, false, mqqt_helper::qos_to_u8(publisher_qos).to_be_bytes()
    ).await {
        debug_eprintln!("{} failed to publish {} to {} with error: {}", analyser_id, mqqt_helper::qos_to_u8(publisher_qos), QOS_TOPIC, _error);
    } else {
        debug_println!("{} successfully published {} to topic: {}", analyser_id, mqqt_helper::qos_to_u8(publisher_qos), QOS_TOPIC);
    }
    // Publish the delay
    if let Err(_error) = analyser.publish(
        DELAY_TOPIC, QoS::ExactlyOnce, false, delay.to_be_bytes()
    ).await {
        debug_eprintln!("{} failed to publish {} to {} with error: {}", analyser_id, delay, DELAY_TOPIC, _error);
    } else {
        debug_println!("{} successfully published {} to topic: {}", analyser_id, delay, DELAY_TOPIC);
    }

    let mut publisher_topics = Vec::with_capacity(instancecount as usize);
    for i in 1..=instancecount {
        let publisher_topic = mqqt_helper::publisher_topic_string(i, publisher_qos, delay);
        publisher_topics.push(publisher_topic);
    }
    // Subscribe to the publisher topics
    if mqqt_helper::subscribe_to_topics(analyser, analyser_id, analyser_qos, &publisher_topics).await.is_err() {
        return ExperimentResult::new(
            mqqt_helper::qos_to_u8(analyser_qos),
            instancecount,
            mqqt_helper::qos_to_u8(publisher_qos),
            delay, 
            Vec::new(),
            SysResult::default()
        );
    }
    // Publisher topic measurements 
    let mut previous_counter: Vec<u64> = vec![0; instancecount as usize];
    let mut message_count: Vec<u64> = vec![0; instancecount as usize];
    let mut out_of_order_count: Vec<u64> = vec![0; instancecount as usize];
    let mut message_times: Vec<Vec<(u64, Instant)>> = vec![Default::default(); instancecount as usize];

    // Mosquitto $SYS broker measurements
    let mut nconnected_clients: u64 = 0;
    let mut heap_size_sum: u64 = 0;
    let mut heap_size_counter: u64 = 0;
    let mut max_heap_size: u64 = 0;
    let mut npub_msgs_recv: u64 = 0;
    let mut npub_msgs_sent: u64 = 0;
    let mut npub_msgs_dropped: u64 = 0;

    // Event loop to handle incoming messages
    let start = std::time::Instant::now();
    while start.elapsed().as_secs() <= SEND_DURATION.as_secs() {
        if let Ok(event) = eventloop.poll().await {
            match event {
                Event::Incoming(Packet::Publish(publish)) => {
                    // Print the payload of the incoming message
                    if publisher_topics.contains(&publish.topic) {
                        let i = mqqt_helper::publisher_topic_instance(&publish.topic).unwrap() - 1;
                        message_count[i] += 1;

                        let counter = mqqt_helper::bytes_to_u64(&publish);
                        if counter < previous_counter[i] {
                            out_of_order_count[i] += 1;
                        }
                        previous_counter[i] = counter;

                        message_times[i].push((counter, std::time::Instant::now()));
                        debug_println!("{} received {} on {}", analyser_id, counter, publish.topic);
                    }
                    else if publish.topic == CLIENTS_CONNECTED_TOPIC {
                        nconnected_clients = mqqt_helper::utf8_to_u64(&publish);
                        debug_println!("Number of connected clients: {}", nconnected_clients);
                    }
                    else if publish.topic == CURRENT_SIZE_TOPIC {                        
                        heap_size_sum += mqqt_helper::utf8_to_u64(&publish);
                        heap_size_counter += 1;
                        debug_println!("Heap size sum: {}", heap_size_sum);
                    }
                    else if publish.topic == MAX_SIZE_TOPIC {
                        max_heap_size = mqqt_helper::utf8_to_u64(&publish);
                        debug_println!("Maximum heap size: {}", max_heap_size);
                    }
                    else if publish.topic == PUB_MSGS_RECEIVED_TOPIC {
                        npub_msgs_recv = mqqt_helper::utf8_to_u64(&publish);
                        debug_println!("Number of publisher messages received: {}", npub_msgs_recv);
                    }
                    else if publish.topic == PUB_MSGS_SENT_TOPIC {
                        npub_msgs_sent = mqqt_helper::utf8_to_u64(&publish);
                        debug_println!("Number of publisher messages sent: {}", npub_msgs_sent);
                    }
                    else if publish.topic == PUB_MSGS_DROPPED_TOPIC {
                        npub_msgs_dropped = mqqt_helper::utf8_to_u64(&publish);
                        debug_println!("Number of publisher messages dropped: {}", npub_msgs_dropped);
                    }
                }
                _ => {}
            }
        }
    }

    // Save topic results
    let mut topic_results = Vec::new();
    for i in 0..instancecount as usize {
        let expected_count = *counters[i].lock().unwrap(); 
        let message_rate = message_count[i] as f64 / SEND_DURATION.as_secs() as f64;
        let loss_rate = compute_loss_rate(expected_count, message_count[i]);
        let out_of_order_rate = compute_out_of_order_rate(message_count[i], out_of_order_count[i]);
        let inter_message_gap = compute_median_inter_message_gap(&message_times[i]);

        let topic = mqqt_helper::publisher_topic_string((i + 1) as u8, publisher_qos, delay);

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

    // Save sys results
    let avg_heap_size = compute_avg_heap_size(heap_size_sum, heap_size_counter);
    let sys_result = SysResult::new(
        nconnected_clients,
        avg_heap_size,
        max_heap_size,
        npub_msgs_recv,
        npub_msgs_sent,
        npub_msgs_dropped
    );

    // Return experiment results
    ExperimentResult::new(
        mqqt_helper::qos_to_u8(analyser_qos),
        instancecount,
        mqqt_helper::qos_to_u8(publisher_qos),
        delay,
        topic_results,
        sys_result
    )
}

fn compute_out_of_order_rate(message_count: u64, out_of_order_count: u64) -> f64 {
    if out_of_order_count == 0 {
        0.0
    } else {
        message_count as f64 / out_of_order_count as f64
    }
}

fn compute_median_inter_message_gap(message_times: &Vec<(u64, Instant)>) -> u64 {
    let len = message_times.len();

    if len <= 1 {
        return 0;
    } 
    
    let mut time_differences = Vec::with_capacity(len - 1);

    for i in 1..len {
        let current_counter = message_times[i].0;
        let prev_counter = message_times[i - 1].0;

        // Only measure consecutive counter-value messages
        if prev_counter + 1 == current_counter {
            time_differences.push(message_times[i].1 - message_times[i - 1].1);
        }        
    }

    // The len might be different since we only consider consecutive messages
    let len = time_differences.len();

    if len == 0 {
        return 0;
    }

    time_differences.sort();

    // Odd number of elements - return the middle element
    if len % 2 == 1 {
        time_differences[len / 2].as_millis() as u64
    } 
    // Even number of elements - return the average of the two middle elements
    else {
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

fn compute_avg_heap_size(heap_size_sum: u64, heap_size_counter: u64) -> u64 {
    if heap_size_counter == 0 {
        0
    } else {
        heap_size_sum / heap_size_counter
    }
}