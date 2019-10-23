use core::{MQTTError, MQTTResult};

pub fn validate_publish(topic: &str) -> MQTTResult<()> {
    if topic.chars().all(|c| (char::is_alphanumeric(c) || c == '/') && c != '#' && c != '+') {
        return Ok(());
    }
    Err(MQTTError::ClientError(format!("invalid_topic_path: {}", topic)))
}

pub fn validate_subscribe(filter: &str) -> MQTTResult<()> {
    let mut peekable = filter.chars().peekable();
    loop {
        match peekable.next() {
            None => return Ok(()),
            Some('#') => {
                if peekable.peek().is_some() {
                    return Err(MQTTError::ClientError(format!("invalid_topic_filter: {}", filter)));
                }
            }
            Some('+') => {
                if let Some(&next) = peekable.peek() {
                    if next != '/' {
                        return Err(MQTTError::ClientError(format!("invalid_topic_filter: {}", filter)));
                    }
                }
            }
            Some('/') => {
                if let Some(&next) = peekable.peek() {
                    if !(next == '+' || next == '/' || next == '#' || char::is_alphanumeric(next)) {
                        return Err(MQTTError::ClientError(format!("invalid_topic_filter: {}", filter)));
                    }
                }
            }
            Some(_) => {
                if let Some(&next) = peekable.peek() {
                    if !(next == '/' || char::is_alphanumeric(next)) {
                        return Err(MQTTError::ClientError(format!("invalid_topic_filter: {}", filter)));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_publish_correct_topics() {
        assert!(validate_publish("a").is_ok());
        assert!(validate_publish("/a").is_ok());
        assert!(validate_publish("/a/").is_ok());
        assert!(validate_publish("/a/b").is_ok());
    }

    #[test]
    fn test_publish_incorrect_topics() {
        assert_eq!(validate_publish("#").err(), Some(MQTTError::ClientError("invalid_topic_path: #".to_string())));
        assert_eq!(validate_publish("/#").err(), Some(MQTTError::ClientError("invalid_topic_path: /#".to_string())));
        assert_eq!(validate_publish("+").err(), Some(MQTTError::ClientError("invalid_topic_path: +".to_string())));
        assert_eq!(validate_publish("/+").err(), Some(MQTTError::ClientError("invalid_topic_path: /+".to_string())));
    }

    #[test]
    fn test_subscribe_correct_topics() {
        assert!(validate_subscribe("/").is_ok());
        assert!(validate_subscribe("/#").is_ok());
        assert!(validate_subscribe("/+").is_ok());
    }
}
