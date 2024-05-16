use std::sync::Arc;

use tokio::task;
use rumqttc::{AsyncClient, EventLoop, Event, Packet, QoS};
use::debug_print::{debug_println, debug_eprintln};

use crate::{create_mqtt_conn, publisher_topic_string, qos_to_u8};
use crate::{INSTANCECOUNT_TOPIC, QOS_TOPIC, DELAY_TOPIC, SEND_DURATION};
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
        let publisher_qos = qos_to_u8(publisher_qos);        
        if let Err(error) = analyser_clone.publish(QOS_TOPIC, analyser_qos, false, publisher_qos.to_be_bytes()).await {
            debug_eprintln!("{} failed to publish {} to {} with error: {}", analyser_id_clone, publisher_qos, QOS_TOPIC, error);
        } else {
            debug_println!("{} successfully published {} to topic: {}", analyser_id_clone, publisher_qos, QOS_TOPIC);
        }

        // // Publish the delay
        if let Err(error) = analyser_clone.publish(DELAY_TOPIC, analyser_qos, false, delay.to_be_bytes()).await {
            debug_eprintln!("{} failed to publish {} to {} with error: {}", analyser_id_clone, delay, DELAY_TOPIC, error);
        } else {
            debug_println!("{} successfully published {} to topic: {}", analyser_id_clone, delay, DELAY_TOPIC);
        }
    });

    let message_rate: f64 = 0.0;
    let loss_rate: f64 = 0.0;
    let out_of_order_rate: f64 = 0.0;
    let inter_message_gap: f64 = 0.0;

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
    ExperimentResult::new(format!("{}{}", qos_to_u8(analyser_qos), publisher_topic), message_rate, loss_rate, out_of_order_rate, inter_message_gap)
}