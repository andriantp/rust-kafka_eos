use anyhow::Result;
use serde_json::Value;
use apache_avro::Schema;

pub fn load_schema_from_file(path: &str) -> Result<String> {
    Ok(std::fs::read_to_string(path)?)
}

pub fn fetch_latest_schema_id(
    registry_url: &str,
    subject: &str,
) -> Result<Option<i32>> {
    let url = format!("{}/subjects/{}/versions/latest", registry_url, subject);

    let resp = reqwest::blocking::get(&url);

    match resp {
        Ok(r) => {
            if r.status().is_success() {
                let json: Value = r.json()?;
                let id = json["id"].as_i64().unwrap() as i32;
                Ok(Some(id))
            } else {
                Ok(None) // Not found → belum pernah register
            }
        }
        Err(_) => Ok(None),
    }
}


pub fn fetch_schema_by_id(registry_url: &str, id: i32) -> Result<Schema> {
    let url = format!("{}/schemas/ids/{}", registry_url, id);

    // 1. GET request
    let resp = reqwest::blocking::get(&url)
        .map_err(|e| anyhow::anyhow!("HTTP error fetching schema: {}", e))?;

    if !resp.status().is_success() {
        return Err(anyhow::anyhow!(
            "Schema Registry returned status {} for id={}",
            resp.status(),
            id
        ));
    }

    // 2. Parse JSON response
    let json: Value = resp.json()
        .map_err(|e| anyhow::anyhow!("Invalid JSON from Schema Registry: {}", e))?;

    let schema_str = json["schema"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("Missing 'schema' field for id={}", id))?;

    // 3. Parse Avro schema
    let schema = Schema::parse_str(schema_str)
        .map_err(|e| anyhow::anyhow!("Failed to parse Avro schema: {}", e))?;

    Ok(schema)
}


pub fn register_schema(
    registry_url: &str,
    subject: &str,
    schema_str: &str,
) -> Result<i32> {
    let client = reqwest::blocking::Client::new();

    let payload = serde_json::json!({
        "schema": schema_str
    });

    let res = client
        .post(format!("{}/subjects/{}/versions", registry_url, subject))
        .json(&payload)
        .send()?;

    if !res.status().is_success() {
        return Err(anyhow::anyhow!(
            "Failed to register schema: HTTP {}",
            res.status()
        ));
    }

    let json: serde_json::Value = res.json()?;
    let id = json["id"].as_i64().unwrap() as i32;
    Ok(id)
}


pub fn ensure_schema_registered(
    registry_url: &str,
    subject: &str,
    schema_str: &str,
) -> Result<i32> {
    if let Some(id) = fetch_latest_schema_id(registry_url, subject)? {
        return Ok(id);
    }

    // Langsung register raw schema
    let id = register_schema(registry_url, subject, schema_str)?;

    Ok(id)
}
