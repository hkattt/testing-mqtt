use std::fs::{self, File};
use std::io::{self, ErrorKind};
use std::path::Path;
use csv::Writer;

use crate::EXPERIMENT_DIR;

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
}

impl ExperimentResult {
    pub fn new(
        analyser_qos: u8,
        instancecount: u8,
        publisher_qos: u8, 
        delay: u64,
        topic_results: Vec<TopicResult>) -> ExperimentResult 
    {  
        ExperimentResult {
            analyser_qos,
            instancecount,
            publisher_qos,
            delay,
            topic_results,
        }
    }    
}

pub fn save(experiment_results: Vec<ExperimentResult>) -> io::Result<()> {
    // Create output directory to experiment results
    if let Err(error) = fs::create_dir(Path::new(EXPERIMENT_DIR)) {
        if error.kind() != ErrorKind::AlreadyExists {
            return Err(error);
        }
    }

    for experiment_result in experiment_results.iter() {
        let result_file = format!(
            "{}/experiment{}-{}-{}-{}.csv", 
            EXPERIMENT_DIR, 
            experiment_result.analyser_qos, experiment_result.instancecount, 
            experiment_result.publisher_qos, experiment_result.delay
        );

        let file = match File::create(result_file) {
            Ok(file) => file,
            Err(error) => return Err(error),
        };

        let mut writer = Writer::from_writer(file);
        
        save_experiment(&mut writer, experiment_result)?;
    }

    Ok(())
}

fn save_experiment(writer: &mut Writer<File>, experiment_result: &ExperimentResult) -> io::Result<()> {
    writer.write_record(&["Topic", "Message Rate", "Loss Rate", "Out of Order Rate", "Inter Message Gap"])?; 
    
    for topic_result in &experiment_result.topic_results {
        writer.write_record(
            &[
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