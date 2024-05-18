use std::sync::Arc;
use std::time::Duration;

use tokio::task;
use rumqttc::{AsyncClient, EventLoop, Event, Packet, QoS};
use::debug_print::{debug_println, debug_eprintln};

use crate::{create_mqtt_conn, publisher_topic_string, qos_to_u8, subscribe_to_topics};
use crate::{INSTANCECOUNT_TOPIC, QOS_TOPIC, DELAY_TOPIC, CURRENT_SIZE_TOPIC, CONNECTIONS_TOPIC, MAX_SIZE_TOPIC, INFLIGHT_TOPIC, DROPPED_TOPIC, CLIENTS_CONNECTED_TOPIC, SEND_DURATION};
use crate::experiment::ExperimentResult;

pub async fn main_analyser(hostname: &str, port: u16) -> Vec<ExperimentResult> {
    let analyser_id = "analyser";

    // 1 to 5 publishers
    let instancecounts = [1, 2, 3, 4, 5];
    // 3 different quality-of-service levels
    let qoss = [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce];
    // 0ms, 1ms, 2ms, 4ms message delay
    let delays = [0, 1, 2, 4];

    let (analyser, mut eventloop) = create_mqtt_conn(analyser_id, hostname, port);
    let analyser = Arc::new(analyser);

    let mut experiment_results = Vec::new();

    for analyser_qos in qoss {
        for instancecount in instancecounts {
            for publisher_qos in qoss {
                for delay in delays {
                    // Small delay to allow the publishers to get ready
                    tokio::time::sleep(Duration::from_secs(1)).await;

                    println!(
                        "\nANALYSER STARTING EXPERIMENT
                        analyser qos: {}
                        instancecount: {}
                        publisher qos: {}
                        delay: {}ms\n",
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

    task::spawn(async move {
        // Publish the instancecount
        if let Err(error) = analyser_clone.publish(INSTANCECOUNT_TOPIC, analyser_qos, false, instancecount.to_be_bytes()).await {
            debug_eprintln!("{} failed to publish {} to {} with error: {}", analyser_id_clone, instancecount, INSTANCECOUNT_TOPIC, error);
        } else {
            debug_println!("{} successfully published {} to topic: {}", analyser_id_clone, instancecount, INSTANCECOUNT_TOPIC);
        }
    
        // Publish the qos
        let publisher_qos = qos_to_u8(publisher_qos);        
        if let Err(error) = analyser_clone.publish(QOS_TOPIC, analyser_qos, false, publisher_qos.to_be_bytes()).await {
            debug_eprintln!("{} failed to publish {} to {} with error: {}", analyser_id_clone, publisher_qos, QOS_TOPIC, error);
        } else {
            debug_println!("{} successfully published {} to topic: {}", analyser_id_clone, publisher_qos, QOS_TOPIC);
        }
    
        // Publish the delay
        if let Err(error) = analyser_clone.publish(DELAY_TOPIC, analyser_qos, false, delay.to_be_bytes()).await {
            debug_eprintln!("{} failed to publish {} to {} with error: {}", analyser_id_clone, delay, DELAY_TOPIC, error);
        } else {
            debug_println!("{} successfully published {} to topic: {}", analyser_id_clone, delay, DELAY_TOPIC);
        }
    });

    if subscribe_to_topics(analyser, analyser_id, analyser_qos, &topics).await.is_err() {
        return ExperimentResult::new(
            format!("{}{}", qos_to_u8(analyser_qos), publisher_topic), 
            -1.0, 
            -1.0, 
            -1.0, 
            -1.0
        );
    }

    let message_rate: f64 = 0.0;
    let loss_rate: f64 = 0.0;
    let out_of_order_rate: f64 = 0.0;
    let inter_message_gap: f64 = 0.0;

    // $SYS Measurements
    // let average_heap_size: u64 = 0;
    // let max_heap_size: u64 = 0;
    // let connections: u64 = 0;

    let start = std::time::Instant::now();

    // Event loop to handle incoming messages
    while let Ok(event) = eventloop.poll().await {
        if start.elapsed().as_secs() > SEND_DURATION {
            break;
        } 
        match event {
            Event::Incoming(Packet::Publish(publish)) => {
                // Print the payload of the incoming message
                if publish.topic == publisher_topic {
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
    
    ExperimentResult::new(
        format!("{}{}", qos_to_u8(analyser_qos), publisher_topic), 
        message_rate, 
        loss_rate, 
        out_of_order_rate, 
        inter_message_gap
    )
}