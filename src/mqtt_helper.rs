use rumqttc::{
    AsyncClient, 
    ClientError, 
    EventLoop, 
    MqttOptions, 
    Publish, 
    QoS
};
use debug_print::{debug_eprintln, debug_println};

use std::time::Duration;
use std::fmt::Debug;

/// Connects a client to the specified MQQT server
/// 
/// # Arguments
/// * `client_id`: Unique ID of the client
/// * `hostname`: Hostname of the MQQT server
/// * `port`: Port that the MQQT server is on
/// * `keep_alive`: Keep alive duration of the connection
/// 
/// # Returns
/// Asynchronous client and event loop for the connection
pub fn create_mqtt_conn(client_id: &str, hostname: &str, port: u16, keep_alive: Duration) -> (AsyncClient, EventLoop) {
    // Create MQTT options
    let mut options = MqttOptions::new(client_id, hostname, port);
    options.set_keep_alive(keep_alive);
    options.set_clean_session(true);

    // Create MQTT client and connection 
    AsyncClient::new(options, 10)
}

/// Subscribes a client to a given list of topics
/// 
/// # Arguments
/// * `client`: Client to subscribe to the topics
/// * `_client_id`: Unique ID of the client
/// * `qos`: Quality-of-service of the subscription
/// * `topics`: Topics to subscribe to 
/// 
/// # Returns
/// Nothing if successfull. `ClientError` if a subscription failed
pub async fn subscribe_to_topics<S>(client: &AsyncClient, _client_id: &str, qos: QoS, topics: &[S]) -> Result<(), ClientError> 
where 
    S: AsRef<str> + Debug,
{
    for topic in topics {
        let topic_str = topic.as_ref();
        if let Err(error) = client.subscribe(topic_str, qos).await {
            debug_eprintln!("{} failed to subscribe to topic {} with error: {}", _client_id, topic_str, error);
            return Err(error);
        } else {
            // TODO: Replace with debug prints
            debug_println!("{} subscribed to topic: {}", _client_id, topic_str);
        }
    }
    Ok(())
}

/// # Arguments
/// * `instance`: Instance number of the publisher
/// * `qos`: Quality-of-service of the publisher connection
/// * `delay`: Message delay used by the publisher
/// 
/// # Returns
/// The publisher topic string i.e. counter/<instance>/<qos>/<delay>
pub fn publisher_topic_string(instance: u8, qos: QoS, delay: u64) -> String {
    format!("counter/{}/{}/{}", instance, qos_to_u8(qos), delay)
}

/// Returns the instance number in a publisher topic string
/// 
/// That is, returns <instance> from 
/// 
/// # Arguments
/// `publisher_topic`: Publisher topic string: counter/<instance>/<qos>/<delay>
/// 
/// # Returns
/// <instance> from the topic String if successful
pub fn publisher_topic_instance(publisher_topic: &str) -> Option<usize> {
    let parts: Vec<&str> = publisher_topic.split('/').collect();

    if let Some(instance_str) = parts.get(1) {
        if let Ok(instance) = instance_str.parse::<usize>() {
            return Some(instance);
        } else {
            debug_eprintln!("The second part of the publisher topic string cannot be parsed into an integer");
            return None;
        }
    } else {
        debug_eprintln!("The publisher topic string has no second part");
        return None;
    }
}

/// # Arguments
/// `qos`: Quality-of-service enum
/// 
/// # Returns
/// Integer corresponding the the given QoS value
pub fn qos_to_u8(qos: QoS) -> u8 {
    match qos {
        QoS::AtMostOnce  => 0, 
        QoS::AtLeastOnce => 1,
        QoS::ExactlyOnce => 2,
    }
}

/// # Arguments
/// `qos`: Quality-of-service integer
/// 
/// # Returns
/// Quality-of-service enum, if the integer is valid
pub fn u8_to_qos(qos: u8) -> Option<QoS> {
    match qos {
        0 => Some(QoS::AtMostOnce),
        1 => Some(QoS::AtLeastOnce),
        2 => Some(QoS::ExactlyOnce),
        _ => None, // Invalid QoS
    }
}

/// # Arguments
/// `qos`: Quality-of-service string
/// 
/// # Returns
/// Quality-of-service enum, if the string is valid
pub fn str_to_qos(qos: &str) -> Option<QoS> {
    match qos {
        "0" => Some(QoS::AtMostOnce),
        "1" => Some(QoS::AtLeastOnce),
        "2" => Some(QoS::ExactlyOnce),
        _ => None, // Invalid QoS
    }
}

/// Converts a payload recevied from a MQQT broker to an integer
/// Interprets the contents as BE bytes
/// 
/// # Arguments
/// `publish`: Publish packet sent from a MQQT broker
/// 
/// # Returns
/// Integer corresponding to the payload contents 
pub fn bytes_to_u64(publish: &Publish) -> u64 {
    let payload = &publish.payload.to_vec();
    let mut array = [0u8; 8];
    let len = payload.len().min(8);
    array[..len].copy_from_slice(&payload[..len]);
    u64::from_be_bytes(array)
}

/// Converts a payload recevied from a MQQT broker to an integer
/// Interprets the contents as a utf8 string
/// 
/// # Arguments
/// `publish`: Publish packet sent from a MQQT broker
/// 
/// # Returns
/// Integer corresponding to the payload contents 
pub fn utf8_to_u64(publish: &Publish) -> u64 {
    let payload = &publish.payload;
    let payload_str = std::str::from_utf8(&payload).unwrap();
    payload_str.parse::<u64>().unwrap()
}