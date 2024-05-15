use tokio::task;
use rumqttc::QoS;

use crate::{create_mqtt_conn, publisher_topic_string};

pub async fn main_publisher(publisher_id: u16, hostname: &str, port: u16, instancecount: u8, qos: QoS, delay: u64) {
    let publisher_id = format!("publisher{}", publisher_id);

    let (publisher, mut eventloop) = create_mqtt_conn(&publisher_id, hostname, port);

    let publisher_topic = publisher_topic_string(instancecount, qos, delay);

    let start = std::time::Instant::now();

    task::spawn(async move {
        for counter in 0.. {
            // Publish the counter value
            if let Err(error) = publisher.publish(&publisher_topic, qos, false, counter.to_string()).await {
                eprintln!("{} failed to publish {} to {} with error: {}", publisher_id, counter, publisher_topic, error);
            } else {
                println!("{} successfully published {} to topic: {}", publisher_id, counter, publisher_topic);
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;
        }
    });

    while start.elapsed().as_secs() <= 60 {
        eventloop.poll().await.unwrap();
    }
}