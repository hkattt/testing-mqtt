use std::time::Duration;
use tokio::{task, time};
use rumqttc::{MqttOptions, AsyncClient, QoS};

pub async fn main_publisher(pub_id: u16, hostname: &str, port: u16, instancecount: u8, qos: QoS, delay: u64) {
    // Broker details
    let hostname = "localhost";
    let port = 1883;

    // Create MQTT options and client
    let mut mqtt_options = MqttOptions::new("publisher", hostname, port);
    mqtt_options.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);
    client.subscribe("someothertopic", QoS::AtLeastOnce).await.unwrap();

    let topic = "example";

    let start = std::time::Instant::now();

    let counter_task = task::spawn(async move {
        for i in 0.. {
            // Publish the counter value
            client.publish(topic, QoS::AtLeastOnce, false, i.to_string())
                .await
                .unwrap();
            println!("Publisher published {} to topic: {}", i.to_string(), topic);
            // Wait for a short duration before publishing the next message
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    });

    while start.elapsed().as_secs() <= 60 {
        eventloop.poll().await.unwrap();
    }
}