use crate::kafka::config::KafkaConfig;
use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord,Producer};
use log::info;


pub struct TxProducer {
    cfg: KafkaConfig,
    pub producer: BaseProducer,
}

impl TxProducer {
    pub fn new(cfg: &KafkaConfig) -> Result<Self> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &cfg.brokers)            
            // └─ Alamat broker Kafka tempat producer mengirim event.

            .set("enable.idempotence", "true")                 
            // └─ Fitur anti-duplikasi bawaan Kafka.
            //    Dengan idempotence = true, Kafka mencegah pengiriman ulang
            //    (retry) menghasilkan duplicate message. Wajib ON untuk EOS.

            .set("transactional.id", &cfg.transactional_id)    
            // └─ Kunci utama Exactly Once Semantics.
            //    Kafka menggunakan transactional.id untuk:
            //      • melacak state transaksi
            //      • memetakan producer ID
            //      • mengelola fencing (mencegah producer lama menulis lagi)
            //    Nilainya HARUS unik per instance.

            .set("acks", "all")                                
            // └─ Kafka hanya menganggap pesan “sukses” jika:
            //      • leader menulis
            //      • seluruh ISR replica menulis
            //    Ini memastikan durability dan mencegah loss.

            .create::<BaseProducer>()?;

        Ok(Self {
            cfg: cfg.clone(),
            producer:producer,
        })
    }

    pub fn run_init_only(&self) -> Result<()> {
        self.init_transactions()?;
        self.begin()?;
        self.commit()?;
        Ok(())
    }

    pub fn init_transactions(&self) -> Result<()> {
        self.producer.init_transactions(None)?;
        Ok(())
    }

    pub fn begin(&self) -> Result<()> {
        self.producer.begin_transaction()?;
        Ok(())
    }

    pub fn commit(&self) -> Result<()> {
        self.producer.commit_transaction(None)?;
        Ok(())
    }

    // pub fn abort(&self) -> Result<()> {
    //     self.producer.abort_transaction(None)?;
    //     Ok(())
    // }

    pub fn send(&self, key: &str, payload: &str) -> Result<()> {
        match self.producer.send(
            BaseRecord::to(&self.cfg.topic_input)
                .payload(payload)
                .key(key),
        ) {
            Ok(_) => {
                info!("📤 Sent (tx): {}", payload);
            }
            Err((err, _record)) => {
                return Err(anyhow::anyhow!("Kafka send error: {}", err));
            }
        }

        Ok(())
    }

    pub fn send_raw_bytes(&self, key: &str,bytes: &[u8]) -> Result<()> {
        match self.producer.send(
            BaseRecord::to(&self.cfg.topic_input)
                .payload(bytes)
                .key(key),
        ) {
            Ok(_) => {
                info!("📤 Sent (tx): raw bytes to {} with topic {}", &self.cfg.topic_input, key);
            }
            Err((err, _record)) => {
                return Err(anyhow::anyhow!("Kafka send error: {}", err));
            }
        }

        Ok(())
    }
    
}
