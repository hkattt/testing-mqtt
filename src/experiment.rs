use std::fs::File;
use std::io;
use csv::Writer;

use crate::RESULT_FILE;

pub struct ExperimentResult {
    id: String,
    message_rate: f64,
    loss_rate: f64,
    out_of_order_rate: f64,
    inter_message_gap: f64,
}

impl ExperimentResult {
    pub fn new(
        id: String, 
        message_rate: f64, 
        loss_rate: f64, 
        out_of_order_rate: f64, 
        inter_message_gap: f64) -> ExperimentResult 
    {  
        ExperimentResult {
            id,
            message_rate,
            loss_rate,
            out_of_order_rate,
            inter_message_gap
        }
    }    
}

pub fn save(experiment_results: Vec<ExperimentResult>) -> io::Result<()> {
    let file = match File::create(RESULT_FILE) {
        Ok(file) => file,
        Err(error) => return Err(error),
    };

    let mut writer = Writer::from_writer(file);

    // Write CSV headers
    writer.write_record(&["", "Message Rate (message/second)", "Loss Rate", "Out-of-order Rate", "Inter-message Gap"])?;

    for experiment_result in experiment_results.iter() {
        writer.write_record(
            &[
                experiment_result.id.clone(),
                experiment_result.message_rate.to_string(),
                experiment_result.loss_rate.to_string(),
                experiment_result.out_of_order_rate.to_string(),
                experiment_result.inter_message_gap.to_string(),
            ]
        )?;
    }
    Ok(())
}