use anyhow::Result;
use apache_avro::{Schema, types::Value};
use crate::kafka::schema::event::SensorEvent;


 // ============== ENCODING ==============
pub fn event_to_avro_value(event: &SensorEvent) -> Result<Value> {
    let record = Value::Record(vec![
        ("sensor".into(), Value::String(event.sensor.clone())),
        ("temp".into(), Value::Double(event.temp)),
        ("humidity".into(), Value::Double(event.humidity)),
        ("time".into(), Value::String(event.time.clone())),
        ("event_id".into(), Value::String(event.event_id.clone())),
        ("producer_id".into(), Value::String(event.producer_id.clone())),
        ("schema_version".into(), Value::Int(event.schema_version)),
        ("ingested_at".into(), Value::String(event.ingested_at.clone())),
    ]);

    Ok(record)
}

pub fn encode_avro(schema: &Schema, value: &Value) -> Result<Vec<u8>> {
    let bytes = apache_avro::to_avro_datum(schema, value.clone())
        .map_err(|e| anyhow::anyhow!("Avro encode error: {}", e))?;

    Ok(bytes)
}


pub fn wrap_confluent_wire_format(schema_id: i32, avro_bytes: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 4 + avro_bytes.len());

    // Byte 0: Magic byte (selalu 0)
    buf.push(0);

    // Byte 1-4: Schema ID (big endian)
    buf.extend_from_slice(&schema_id.to_be_bytes());

    // Sisanya: Avro payload
    buf.extend_from_slice(avro_bytes);

    buf
}


// ============== DECODING ==============
pub fn avro_value_to_event(value: Value) -> Result<SensorEvent> {
    if let Value::Record(fields) = value {
        Ok(SensorEvent {
            sensor:         get_string(&fields, "sensor")?,
            temp:           get_f64(&fields, "temp")?,
            humidity:       get_f64(&fields, "humidity")?,
            time:           get_string(&fields, "time")?,
            event_id:       get_string(&fields, "event_id")?,
            producer_id:    get_string(&fields, "producer_id")?,
            schema_version: get_i32(&fields, "schema_version")?,
            ingested_at:    get_string(&fields, "ingested_at")?,
        })
    } else {
        Err(anyhow::anyhow!("Expected Avro Record"))
    }
}


pub fn decode_avro(schema: &Schema, avro_payload: &[u8]) -> anyhow::Result<Value> {
    let value = apache_avro::from_avro_datum(schema, &mut &avro_payload[..], None)
        .map_err(|e| anyhow::anyhow!("Avro decode error: {}", e))?;

    Ok(value)
}


pub fn unwrap_confluent_wire_format(bytes: &[u8]) -> anyhow::Result<(i32, &[u8])> {
    if bytes.len() < 5 {
        return Err(anyhow::anyhow!("Invalid Avro wire format"));
    }

    let magic = bytes[0];
    if magic != 0 {
        return Err(anyhow::anyhow!("Magic byte must be 0"));
    }

    let schema_id = i32::from_be_bytes(bytes[1..5].try_into().unwrap());
    let avro_payload = &bytes[5..];

    Ok((schema_id, avro_payload))
}


// ============== HELPERS FOR SCHEMA REGISTRY ==============
fn get_string(fields: &[(String, Value)], key: &str) -> Result<String> {
    match fields.iter().find(|(k, _)| k == key).map(|(_, v)| v) {
        Some(Value::String(s)) => Ok(s.clone()),
        _ => Err(anyhow::anyhow!("Field '{}' must be string", key)),
    }
}

fn get_f64(fields: &[(String, Value)], key: &str) -> Result<f64> {
    match fields.iter().find(|(k, _)| k == key).map(|(_, v)| v) {
        Some(Value::Double(f)) => Ok(*f),
        Some(Value::Float(f)) => Ok(*f as f64),
        _ => Err(anyhow::anyhow!("Field '{}' must be double", key)),
    }
}

fn get_i32(fields: &[(String, Value)], key: &str) -> Result<i32> {
    match fields.iter().find(|(k, _)| k == key).map(|(_, v)| v) {
        Some(Value::Int(i)) => Ok(*i),
        Some(Value::Long(l)) => Ok(*l as i32),
        _ => Err(anyhow::anyhow!("Field '{}' must be int", key)),
    }
}