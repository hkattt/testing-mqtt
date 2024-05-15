use std::time::Duration;
use rumqttc::{MqttOptions, AsyncClient, Event, Packet, QoS};

pub async fn main_analyser(hostname: &str, port: u16, instancecount: u8, qos: QoS, delay: u64) {
    // Create MQTT options and client
    let mut mqtt_options = MqttOptions::new("analyser", hostname, port);
    mqtt_options.set_keep_alive(Duration::from_secs(5));
    mqtt_options.set_clean_session(true);

    let (mut client, mut eventloop) = AsyncClient::new(mqtt_options, 10);
    
    let topic = "example";

    // Subscribe to the topic
    client.subscribe(topic, QoS::AtLeastOnce).await.unwrap();

    println!("Analyser subscribed to topic: {}", topic);

    // Event loop to handle incoming messages
    while let Ok(event) = eventloop.poll().await {
        match event {
            Event::Incoming(Packet::Publish(publish)) => {
                // Print the payload of the incoming message
                if publish.topic == topic {
                    println!("Received: {:?}", publish.payload);
                }
            }
            _ => {}
        }
    }
}
