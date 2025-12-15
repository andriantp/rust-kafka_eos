use std::env;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct KafkaConfig {
    pub brokers: String,
    pub topic_input: String,
    pub topic_output: String,
    pub topic_dlq: String,
    pub group_id: String,
    pub transactional_id: String,
    pub schema_file: String,
    pub schema_registry_url: String,
}

impl KafkaConfig {
    pub fn new() -> Self {
        let cfg = Self {
            brokers: env::var("KAFKA_BROKER")
                .unwrap_or_else(|_| "localhost:9092".to_string()),

            topic_input: env::var("KAFKA_TOPIC_INPUT")
                .unwrap_or_else(|_| "rust-eos".to_string()),

            topic_output: env::var("KAFKA_TOPIC_OUTPUT")
                .unwrap_or_else(|_| "rust-eos-out".to_string()),

            topic_dlq: env::var("KAFKA_TOPIC_DLQ")
                .unwrap_or_else(|_| "rust-eos-dlq".to_string()),

            group_id: env::var("KAFKA_GROUP_ID")
                .unwrap_or_else(|_| "rust-group-1".to_string()),

            transactional_id: env::var("KAFKA_TRANSACTIONAL_ID")
                .unwrap_or_else(|_| "rust-eos-producer-1".to_string()),

            schema_file: env::var("SCHEMA_FILE")
                .unwrap_or_else(|_| "src/kafka/schema/sensor_event.avsc".into()),    
                
            schema_registry_url: env::var("SCHEMA_REGISTRY_URL")
                .unwrap_or_else(|_| "http://localhost:8081".to_string()),
        };

        log::info!("🧩 Loaded Kafka config: {:?}", cfg);
        cfg
    }
}
