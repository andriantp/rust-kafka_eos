mod kafka;

use anyhow::Result;
use clap::{Parser, ValueEnum};
use dotenvy;
use env_logger::Env;
use log::info;

use rand::Rng;
use chrono::Utc;
use rdkafka::producer::Producer;
// use std::{thread, time::Duration};

use kafka::config::KafkaConfig;

#[derive(ValueEnum, Clone, Debug)]
enum Mode {
    TxProducer,
    EosConsumer,
    AvProducer,
    AvroConsumer,  
}

#[derive(Parser)]
struct Cli {
    #[arg(value_enum)]
    mode: Mode,
}

fn main() -> Result<()> {
    // Init logger
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    // Load .env
    if dotenvy::dotenv().is_err() {
        panic!("⚠️ .env file not found");
    }

    let cfg = KafkaConfig::new();
    let cli = Cli::parse();

    match cli.mode {
        Mode::TxProducer => run_transactional_producer(cfg)?,
        Mode::EosConsumer => run_eos_consumer(cfg)?,
        Mode::AvProducer => run_avro_producer(cfg)?,
        Mode::AvroConsumer => run_avro_consumer(cfg)?,
    }

    Ok(())
}


fn run_transactional_producer(cfg: KafkaConfig) -> Result<()> {
    use kafka::producer::transactional::TxProducer;

    let producer = TxProducer::new(&cfg)?;
    producer.run_init_only()?;

    info!("✨ Transaction init OK");

    // loop {
        let msg = make_random_json();
        // let msg="";
        producer.begin()?;
        producer.send( "key1", &msg)?;
        // producer.send_raw_bytes("raw", &[0xFF])?;
        producer.commit()?;

        if let Err(e) = producer.producer.flush(std::time::Duration::from_secs(1)) {
            log::warn!("Flush error (ignored): {}", e);
        }


        // thread::sleep(Duration::from_secs(3));
    // }

    Ok(())
}


fn run_eos_consumer(cfg: KafkaConfig) -> Result<()> {
    use kafka::consumer::eos::EosConsumer;

    let consumer = EosConsumer::new(&cfg)?;
    consumer.subscribe(&cfg.topic_input)?;

    println!("📥 EOS consumer started...");

    loop {
        consumer.poll_once()?;
    }
}


fn make_random_json() -> String {
    let temp = rand::thread_rng().gen_range(20.0..30.0);
    let humidity = rand::thread_rng().gen_range(50.0..80.0);
    let timestamp = Utc::now().to_rfc3339();

    format!(
        r#"{{"sensor":"A1","temp":{:.2},"humidity":{:.2},"time":"{}"}}"#,
        temp, humidity, timestamp
    )
    
}


fn run_avro_producer(cfg: KafkaConfig) -> Result<()> {
    use kafka::producer::avro::AvProducer;
    use kafka::producer::transactional::TxProducer;
    use kafka::schema::event::SensorEvent;

    // 1. init AvProducer
    let av = AvProducer::new(&cfg)?;

    // 2. init TxProducer
    let tx = TxProducer::new(&cfg)?;
    tx.run_init_only()?;

    // 3. generate event
    let event = SensorEvent::generate_random("A1");
    info!("Event to send: {:?}", event);
    let bytes = av.encode_event(&event)?;

    tx.begin()?;
    tx.send_raw_bytes("key-1", &bytes)?;
    tx.commit()?;

    if let Err(e) = tx.producer.flush(std::time::Duration::from_secs(1)) {
        log::warn!("Flush error (ignored): {}", e);
    }

    log::info!("📤 Avro event sent & committed!");

    Ok(())
}


fn run_avro_consumer(cfg: KafkaConfig) -> Result<()> {
    use kafka::consumer::avro::AvroConsumer;

    let consumer = AvroConsumer::new(&cfg)?;
    consumer.subscribe(&cfg.topic_input)?;

    println!("📥 Avro Consumer started...");
    loop {
        consumer.poll_once()?;
    }
}
