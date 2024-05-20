use std::fs::{self, File};
use std::io::{self, ErrorKind};
use std::path::Path;
use csv::Writer;

use crate::EXPERIMENT_DIR;

#[derive(Default)]
pub struct SysResult {
    nconnected_clients: u64, 
    avg_heap_size: u64,
    max_heap_size: u64,
    npub_msgs_recv: u64, 
    npub_msgs_sent: u64,
    npub_msgs_dropped: u64,
}

impl SysResult {
    pub fn new(
        nconnected_clients: u64, 
        avg_heap_size: u64,
        max_heap_size: u64,
        npub_msgs_recv: u64, 
        npub_msgs_sent: u64,
        npub_msgs_dropped: u64) -> SysResult
    {
        SysResult {
            nconnected_clients,
            avg_heap_size,
            max_heap_size,
            npub_msgs_recv,
            npub_msgs_sent,
            npub_msgs_dropped
        }
    }
}

#[derive(Default)]
pub struct TopicResult {
    topic: String, 
    message_rate: f64, 
    loss_rate: f64, 
    out_of_order_rate: f64, 
    inter_message_gap: u64,
}

impl TopicResult {
    pub fn new(
        topic: String, 
        message_rate: f64,
        loss_rate: f64,
        out_of_order_rate: f64, 
        inter_message_gap: u64) -> TopicResult
    {
        TopicResult {
            topic,
            message_rate,
            loss_rate,
            out_of_order_rate,
            inter_message_gap
        }
    }
}

pub struct ExperimentResult {
    analyser_qos: u8,
    instancecount: u8, 
    publisher_qos: u8,
    delay: u64,
    topic_results: Vec<TopicResult>,
    sys_result: SysResult,
}

impl ExperimentResult {
    pub fn new(
        analyser_qos: u8,
        instancecount: u8,
        publisher_qos: u8, 
        delay: u64,
        topic_results: Vec<TopicResult>,
        sys_result: SysResult) -> ExperimentResult 
    {  
        ExperimentResult {
            analyser_qos,
            instancecount,
            publisher_qos,
            delay,
            topic_results,
            sys_result
        }
    }    
}

pub fn save_experiment_results(
        experiment_results: Vec<ExperimentResult>, 
        topic_results_file: &str, 
        sys_results_file: &str) -> io::Result<()> 
    {
    // Create output directory to experiment results
    if let Err(error) = fs::create_dir(Path::new(EXPERIMENT_DIR)) {
        if error.kind() != ErrorKind::AlreadyExists {
            return Err(error);
        }
    }

    let topic_results_file_path = format!("{}/{}", EXPERIMENT_DIR, topic_results_file);
    let sys_results_file_path = format!("{}/{}", EXPERIMENT_DIR, sys_results_file);

    let topic_results_file = match File::create(topic_results_file_path) {
        Ok(file) => file,
        Err(error) => return Err(error),
    };

    let sys_results_file = match File::create(sys_results_file_path) {
        Ok(file) => file,
        Err(error) => return Err(error),
    };

    let mut topic_results_write = Writer::from_writer(topic_results_file);
    let mut sys_results_write = Writer::from_writer(sys_results_file);

    // Write CSV headers
    topic_results_write.write_record(&[
        "Experiment ID", "Analyser QoS", "Instance Count", "Publisher QoS", "Delay (ms)", "Topic", "Message Rate", 
        "Loss Rate", "Out of Order Rate", "Inter Message Gap"
    ])?; 

    sys_results_write.write_record(&[
        "Experiment ID", "Analyser QoS", "Instance Count", "Publisher QoS", "Delay (ms)", "Number of Connected Clients", 
        "Average Heap Size", "Max Heap Size", "Number of Publisher Messages Received", "Number of Publisher Messages Sent", 
        "Number of Publisher Messages Dropped"
    ])?;

    for experiment_result in experiment_results.iter() {
        let experiment_id = format!(
            "experiment{}-{}-{}-{}",
            experiment_result.analyser_qos, experiment_result.instancecount,
            experiment_result.publisher_qos, experiment_result.delay
        );
        save_topic_results(&mut topic_results_write, &experiment_id, &experiment_result)?;
        save_sys_result(&mut sys_results_write, &experiment_id, &experiment_result)?;
    }

    Ok(())
}

fn save_topic_results(writer: &mut Writer<File>, experiment_id: &str, experiment_result: &ExperimentResult) -> io::Result<()> {    
    let topic_results = &experiment_result.topic_results;

    for topic_result in topic_results {
        writer.write_record(
            &[
                experiment_id.to_string(),
                experiment_result.analyser_qos.to_string(), 
                experiment_result.instancecount.to_string(),
                experiment_result.publisher_qos.to_string(), 
                experiment_result.delay.to_string(),
                topic_result.topic.clone(),
                topic_result.message_rate.to_string(),
                topic_result.loss_rate.to_string(),
                topic_result.out_of_order_rate.to_string(),
                topic_result.inter_message_gap.to_string(),
            ]
        )?;
    }

    // Flush the writer to ensure all data is written
    writer.flush()?;

    Ok(())  
}

fn save_sys_result(writer: &mut Writer<File>, experiment_id: &str, experiment_result: &ExperimentResult) -> io::Result<()> {    
    let sys_result = &experiment_result.sys_result;

    writer.write_record(
        &[
            experiment_id.to_string(),
            experiment_result.analyser_qos.to_string(), 
            experiment_result.instancecount.to_string(),
            experiment_result.publisher_qos.to_string(), 
            experiment_result.delay.to_string(),
            sys_result.nconnected_clients.to_string(),
            sys_result.avg_heap_size.to_string(),
            sys_result.max_heap_size.to_string(),
            sys_result.npub_msgs_recv.to_string(),
            sys_result.npub_msgs_sent.to_string(),
            sys_result.npub_msgs_dropped.to_string()
        ]
    )?;

    // Flush the writer to ensure all data is written
    writer.flush()?;

    Ok(())  
}