use uuid::Uuid;

pub(crate) fn new_uuid_id() -> String {
    Uuid::new_v4().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_uuid_id_returns_unique_valid_uuids() {
        let first = new_uuid_id();
        let second = new_uuid_id();

        assert_ne!(first, second);
        assert!(Uuid::parse_str(&first).is_ok());
        assert!(Uuid::parse_str(&second).is_ok());
    }
}
