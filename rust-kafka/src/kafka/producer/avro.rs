use anyhow::Result;
use apache_avro::Schema;

use crate::kafka::schema::event::SensorEvent;
use crate::kafka::schema::registry::{
    load_schema_from_file,
    ensure_schema_registered,
};
use crate::kafka::schema::avro::{
    event_to_avro_value,
    encode_avro,
    wrap_confluent_wire_format,
};
use crate::kafka::config::KafkaConfig;

pub struct AvProducer {
    pub schema: Schema,
    pub schema_id: i32,
}

impl AvProducer {
    pub fn new(cfg: &KafkaConfig) -> Result<Self> {
        // 1. Load schema file via helper
        let raw_schema = load_schema_from_file(&cfg.schema_file)?;

        // 2. Parse Avro schema
        let schema = apache_avro::Schema::parse_str(&raw_schema)
            .map_err(|e| anyhow::anyhow!("Invalid Avro schema: {}", e))?;

        // 3. Register schema (if not exists) or fetch ID
        // let subject = "SensorEvent-value";  // standard subject naming
        let subject = format!("{}-value", cfg.topic_input);
        let schema_id = ensure_schema_registered(
            &cfg.schema_registry_url,
            &subject,
            &raw_schema,
        )?;

        Ok(Self {
            schema,
            schema_id,
        })
    }

    pub fn encode_event(&self, event: &SensorEvent) -> Result<Vec<u8>> {
        // 1. Convert struct → Avro Value
        let value = event_to_avro_value(event)?;

        // 2. Encode Avro Value → binary Avro
        let avro_bytes = encode_avro(&self.schema, &value)?;

        // 3. Wrap into Confluent wire format
        let final_bytes = wrap_confluent_wire_format(self.schema_id, &avro_bytes);

        Ok(final_bytes)
    }
}
