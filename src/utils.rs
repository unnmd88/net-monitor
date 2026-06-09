use chrono::Local;

const TIME_FMT: &str = "%Y-%m-%d %H:%M:%S%.3f";

pub fn get_fmt_current_time() -> String {
    Local::now().format(TIME_FMT).to_string()
}

pub fn validate_oids(oids: &[String]) -> Result<(), String> {
    if oids.is_empty() {
        return Err("SNMP OID list is empty".to_string());
    }
    for (pos, oid) in oids.iter().enumerate() {
        if oid.split('.').any(|p| p.parse::<u32>().is_err()) {
            return Err(format!("Invalid OID: '{}' at position {}", oid, pos));
        }
    }
    Ok(())
}
