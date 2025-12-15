use anyhow::Result;
use rdkafka::consumer::{BaseConsumer, Consumer, CommitMode};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::config::ClientConfig;
use rdkafka::Message;
use log::info;

use crate::kafka::config::KafkaConfig;

pub struct EosConsumer {
    cfg: KafkaConfig,
    consumer: BaseConsumer,
    producer: BaseProducer,
}

impl EosConsumer {
    pub fn new(cfg: &KafkaConfig) -> Result<Self> {
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", &cfg.brokers)          
            // └─ Alamat broker Kafka.

            .set("group.id", &cfg.group_id)                  
            // └─ Group ID. Jika group baru, Kafka mulai dari earliest (sesuai config).

            .set("enable.auto.commit", "false")              
            // └─ EOS consumer harus manual offset management.
            //    Auto-commit = riskan → bisa commit pesan yang belum diproses.

            .set("isolation.level", "read_committed")        
            // └─ KUNCI EXACTLY-ONCE. 
            //    Consumer hanya membaca pesan dari transaksi yang commit.
            //    Pesan uncommitted atau aborted akan dilewati.

            .set("auto.offset.reset", "earliest")            
            // └─ Jika belum ada offset (group ID baru), mulai membaca dari awal topic.
            //    Berguna untuk testing agar consumer bisa membaca pesan lama.

            .create::<BaseConsumer>()?;

        let producer = ClientConfig::new()
            .set("bootstrap.servers", &cfg.brokers)
            .set("acks", "all")
            .create::<BaseProducer>()?;

        Ok(Self {
            cfg: cfg.clone(),
            consumer: consumer,
            producer:producer,
        })
    }

    pub fn subscribe(&self, topic: &str) -> Result<()> {
        self.consumer.subscribe(&[topic])?;
        Ok(())
    }

    pub fn poll_once(&self) -> Result<()> {
        if let Some(result) = self.consumer.poll(std::time::Duration::from_millis(500)) {
            match result {
                Ok(msg) => {
                    // let payload = msg.payload_view::<str>()
                    //     .unwrap_or(Ok("<invalid utf8>"))?;

                    // println!("📥 Received (EOS): {}", payload);
                    // self.consumer.commit_message(&msg, CommitMode::Sync)?;
                    
                    // decode
                    let payload = match msg.payload_view::<str>() {
                        Some(Ok(v)) => v,
                        _ => {
                            // payload asli untuk DLQ
                            if let Some(bytes) = msg.payload() {
                                let raw = String::from_utf8_lossy(bytes);
                                info!(
                                    "⚠️ Decode error → DLQ | topic={} partition={} offset={} raw={}",
                                    msg.topic(),
                                    msg.partition(),
                                    msg.offset(),
                                    raw
                                );
                                self.send_to_dlq(&raw)?;
                            } else {
                                info!(
                                    "⚠️ Empty payload → DLQ | topic={} partition={} offset={}",
                                    msg.topic(),
                                    msg.partition(),
                                    msg.offset()
                                );
                                self.send_to_dlq("<empty-payload>")?;
                            }
                            self.consumer.commit_message(&msg, CommitMode::Sync)?;
                            return Ok(());
                        }
                    };
                    println!("📥 Received (EOS): {}", payload);

                    // STEP 1: UTF-8 decode
                    let payload = match msg.payload_view::<str>() {
                        Some(Ok(v)) => v,
                        _ => {
                            if let Some(bytes) = msg.payload() {
                                let raw = String::from_utf8_lossy(bytes);
                                self.send_to_dlq(&raw)?;
                            } else {
                                self.send_to_dlq("<empty-payload-utf8>")?;
                            }
                            self.consumer.commit_message(&msg, CommitMode::Sync)?;
                            return Ok(());
                        }
                    };

                    // STEP 2A. Empty string?
                    if payload.trim().is_empty() {
                        info!("⚠️ Empty payload → DLQ");
                        self.send_to_dlq("<empty-payload>")?;
                        self.consumer.commit_message(&msg, CommitMode::Sync)?;
                        return Ok(());
                    }

                    // STEP 2B: JSON validation
                    if serde_json::from_str::<serde_json::Value>(payload).is_err() {
                        info!("⚠️ JSON invalid → DLQ: {}", payload);
                        self.send_to_dlq(payload)?;
                        self.consumer.commit_message(&msg, CommitMode::Sync)?;
                        return Ok(());
                    }

                    // routing: good
                    self.send_to_output(payload)?;
                    self.consumer.commit_message(&msg, CommitMode::Sync)?;
                }
                Err(err) => {
                    println!("❌ Kafka error: {}", err);
                }
            }
        } 

        Ok(())
    }


    pub fn send_to_output(&self,  payload: &str) -> Result<()> {
        match self.producer.send(
            BaseRecord::to(&self.cfg.topic_output)
                .payload(payload)
                .key("ok"),
        ) {
            Ok(_) => {
                info!("📤 Sent (output): {}", payload);
            }
            Err((err, _record)) => {
                return Err(anyhow::anyhow!("Kafka send output error: {}", err));
            }
        }
        
        // flush supaya message pasti terkirim
        self.producer.flush(None)?;
        Ok(())
    }

    pub fn send_to_dlq(&self,  payload: &str) -> Result<()> {
        match self.producer.send(
            BaseRecord::to(&self.cfg.topic_dlq)
                .payload(payload)
                .key("error"),
        ) {
            Ok(_) => {
                info!("📤 Sent (dlq): {}", payload);
            }
            Err((err, _record)) => {
                return Err(anyhow::anyhow!("Kafka send dlq error: {}", err));
            }
        }

        // flush supaya message pasti terkirim
        self.producer.flush(None)?;
        Ok(())
    }

}
