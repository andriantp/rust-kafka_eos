use chrono::Utc;
use uuid::Uuid;
use rand::Rng;

#[derive(Debug, Clone)]
pub struct SensorEvent {
    pub sensor: String,
    pub temp: f64,
    pub humidity: f64,
    pub time: String,           // waktu event
    pub event_id: String,       // UUID unik tiap event
    pub producer_id: String,    // nama producer rust
    pub schema_version: i32,    // default: 1
    pub ingested_at: String,    // timestamp sekarang
}

impl SensorEvent {
    pub fn new(sensor: &str, temp: f64, humidity: f64) -> Self {
        Self {
            sensor: sensor.to_string(),
            temp,
            humidity,

            // sekarang default: waktu di-produce
            time: Utc::now().to_rfc3339(),

            event_id: Uuid::new_v4().to_string(),
            producer_id: "rust-producer".into(),

            schema_version: 1,
            ingested_at: Utc::now().to_rfc3339(),
        }
    }

    pub fn generate_random(sensor: &str) -> Self {
        let mut rng = rand::thread_rng();

        let temp = rng.gen_range(20.0..30.0);
        let humidity = rng.gen_range(50.0..80.0);

        SensorEvent::new(sensor, temp, humidity)
    }
}

