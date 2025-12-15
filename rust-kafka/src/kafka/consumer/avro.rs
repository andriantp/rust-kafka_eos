
use anyhow::Result;
use log::warn;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, CommitMode};
use rdkafka::Message;

use crate::kafka::config::KafkaConfig;
use crate::kafka::schema::avro::{
    unwrap_confluent_wire_format, decode_avro, avro_value_to_event
};
use crate::kafka::schema::registry::fetch_schema_by_id;



/// EOS-aware Avro consumer that decodes Avro payloads
pub struct AvroConsumer {
    consumer: BaseConsumer,
    cfg: KafkaConfig,
}

impl AvroConsumer {
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

        Ok(Self {
            cfg: cfg.clone(),
            consumer: consumer,
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
                // STEP 1 — ambil payload binary, bukan UTF-8
                let payload = match msg.payload() {
                    Some(bytes) => bytes,   // <-- binary raw
                    None => {
                        warn!("⚠️ Empty payload");
                        self.consumer.commit_message(&msg, CommitMode::Sync)?;
                        return Ok(());
                    }
                };

                // STEP 2 — unwrap confluent wire format
                let (schema_id, avro_payload) = match unwrap_confluent_wire_format(payload) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!("⚠️ Wire format error: {}", e);
                        self.consumer.commit_message(&msg, CommitMode::Sync)?;
                        return Ok(());
                    }
                };

                // STEP 3 — ambil schema by id
                let schema = match fetch_schema_by_id(&self.cfg.schema_registry_url, schema_id) {
                    Ok(s) => s,
                    Err(e) => {
                        warn!("⚠️ Failed fetch schema id {}: {}", schema_id, e);
                        self.consumer.commit_message(&msg, CommitMode::Sync)?;
                        return Ok(());
                    }
                };

                // STEP 4 — decode avro bytes
                let value = match decode_avro(&schema, avro_payload) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!("⚠️ Avro decode error: {}", e);
                        self.consumer.commit_message(&msg, CommitMode::Sync)?;
                        return Ok(());
                    }
                };

                // STEP 5 — convert Value → SensorEvent
                let event = match avro_value_to_event(value) {
                    Ok(ev) => ev,
                    Err(e) => {
                        warn!("⚠️ Conversion error: {}", e);
                        self.consumer.commit_message(&msg, CommitMode::Sync)?;
                        return Ok(());
                    }
                };

                println!("📥 Decoded event = {:?}", event);

                self.consumer.commit_message(&msg, CommitMode::Sync)?;
            }

            Err(err) => warn!("Kafka error: {}", err),
        }
    }
    Ok(())
}


}
