use std::time::{Duration, SystemTime};

use rumqttc::{Client, ClientError, Connection, MqttOptions, QoS};
use std::thread;

fn main() {
    // Broker details
    let hostname = "localhost";
    let port = 1883;

    // Analyser and publisher ids
    let analyser_id = "analyser";
    let pub_id = "client1";

    let instancecount = 1;
    let qos = QoS::AtMostOnce;
    let delay = 1000; // ms

    let pub_topic = publisher_topic_string(instancecount, qos, delay);

    let (analyser, mut analyser_conn) = create_mqtt_conn(analyser_id, hostname, port);
    match analyser.subscribe(&pub_topic, QoS::AtMostOnce) {
        Ok(()) => println!("Analyser sucessfully subscribed to the publisher topic: {}", pub_topic),
        Err(error) => eprintln!("Unable to subscribe to the publisher topic: {} with error: {}", pub_topic, error),
    }

    let (pub1, client_conn) = create_mqtt_conn(pub_id, hostname, port);
    match pub1.subscribe(&pub_topic, qos) {
        Ok(()) => println!("Publisher 1 sucessfully subscribed to the publisher topic: {}", pub_topic),
        Err(error) => eprintln!("Unable to subscribe to the publisher topic: {} with error: {}", pub_topic, error),
    }

    thread::spawn(move || {
        match publish_counter(pub1, &pub_topic, qos, delay) {
            Ok(()) => (),
            Err(error) => eprintln!("Error occured publishing counter: {}", error),
        }
    });

    for (i, notficiation) in analyser_conn.iter().enumerate() {
        println!("Notification = {:?}", notficiation);
    }
}

fn create_mqtt_conn(client_id: &str, hostname: &str, port: u16) -> (Client, Connection) {
    // Create MQTT options
    let mut options = MqttOptions::new(client_id, hostname, port);
    options.set_keep_alive(Duration::from_secs(5));

    // Create MQTT client and connection 
    Client::new(options, 10)
}

fn publish_counter(client: Client, topic: &str, qos: QoS, delay: u64) -> Result<(), ClientError>{
    let mut counter = 0;
    let start_time = SystemTime::now();

    while start_time.elapsed().unwrap().as_secs() < 60 {
        client.publish(topic, qos, false, counter.to_string())?;
        counter += 1;
        thread::sleep(Duration::from_millis(delay));
    }
    Ok(())
}

fn publisher_topic_string(instancecount: u8, qos: QoS, delay: u64) -> String {
    format!("counter/{}/{}/{}", instancecount, qos_to_u8(qos), delay)
}

fn qos_to_u8(qos: QoS) -> u8 {
    match qos {
        QoS::AtMostOnce  => 0, 
        QoS::AtLeastOnce => 1,
        QoS::ExactlyOnce => 2,
    }
}
