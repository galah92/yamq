use bytes::Bytes;

const TOPIC_SEPARATOR: char = '/';
const MULTI_LEVEL_WILDCARD: char = '#';
const SINGLE_LEVEL_WILDCARD: char = '+';
const MAX_TOPIC_LEN_BYTES: usize = 65_535;

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum TopicParseError {
    #[error("topic cannot be empty")]
    EmptyTopic,
    #[error("topic cannot exceed 65535 bytes")]
    TopicTooLong,
    #[error("topic cannot contain # wildcard anywhere but the last level")]
    MultilevelWildcardNotAtEnd,
    #[error("topic must have a wildcard in a separate level")]
    InvalidWildcardLevel,
    #[error("topic cannot contain wildcards or null characters")]
    WildcardOrNullInTopic,
    #[error("topic must be valid UTF-8")]
    Utf8Error(#[from] std::string::FromUtf8Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topic(String);

impl TryFrom<String> for Topic {
    type Error = TopicParseError;

    fn try_from(topic: String) -> Result<Self, Self::Error> {
        // Topics cannot be empty
        if topic.is_empty() {
            return Err(TopicParseError::EmptyTopic);
        }

        // Topics cannot exceed the byte length in the MQTT spec
        if topic.len() > MAX_TOPIC_LEN_BYTES {
            return Err(TopicParseError::TopicTooLong);
        }

        // Topics cannot contain wildcards or null characters
        let topic_contains_wildcards = topic.contains(|x: char| {
            x == SINGLE_LEVEL_WILDCARD || x == MULTI_LEVEL_WILDCARD || x == '\0'
        });
        if topic_contains_wildcards {
            return Err(TopicParseError::WildcardOrNullInTopic);
        }

        Ok(Topic(topic))
    }
}

impl TryFrom<&str> for Topic {
    type Error = TopicParseError;

    fn try_from(topic: &str) -> Result<Self, Self::Error> {
        Topic::try_from(topic.to_string())
    }
}

impl TryFrom<Bytes> for Topic {
    type Error = TopicParseError;

    fn try_from(topic: Bytes) -> Result<Self, Self::Error> {
        let topic = String::from_utf8(topic.to_vec())?;
        Topic::try_from(topic)
    }
}

impl AsRef<str> for Topic {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Topic {
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl std::fmt::Display for Topic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum TopicFilterParseError {
    #[error("topic cannot be empty")]
    EmptyTopicFilter,
    #[error("topic cannot exceed 65535 bytes")]
    TopicFilterTooLong,
    #[error("topic cannot contain # wildcard anywhere but the last level")]
    MultilevelWildcardNotAtEnd,
    #[error("topic must have a wildcard in a separate level")]
    InvalidWildcardLevel,
    #[error("topic cannot contain wildcards or null characters")]
    WildcardOrNullInTopicFilter,
    #[error("topic must be valid UTF-8")]
    Utf8Error(#[from] std::string::FromUtf8Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicFilter(String);

impl TryFrom<String> for TopicFilter {
    type Error = TopicFilterParseError;

    fn try_from(filter: String) -> Result<Self, Self::Error> {
        // Filters and topics cannot be empty
        if filter.is_empty() {
            return Err(TopicFilterParseError::EmptyTopicFilter);
        }

        // Assert no null character U+0000
        if filter.contains('\0') {
            return Err(TopicFilterParseError::WildcardOrNullInTopicFilter);
        }

        // Filters cannot exceed the byte length in the MQTT spec
        if filter.len() > MAX_TOPIC_LEN_BYTES {
            return Err(TopicFilterParseError::TopicFilterTooLong);
        }

        // Multi-level wildcards can only be at the end of the topiclevel_contains_wildcard
        if let Some(pos) = filter.rfind(MULTI_LEVEL_WILDCARD) {
            if pos != filter.len() - 1 {
                return Err(TopicFilterParseError::MultilevelWildcardNotAtEnd);
            }
        }

        for level in filter.split(TOPIC_SEPARATOR) {
            let level_contains_wildcard =
                level.contains(|x: char| x == SINGLE_LEVEL_WILDCARD || x == MULTI_LEVEL_WILDCARD);
            if level_contains_wildcard {
                // Any wildcards on a particular level must be specified on their own
                if level.len() > 1 {
                    return Err(TopicFilterParseError::InvalidWildcardLevel);
                }
            }
        }

        Ok(TopicFilter(filter))
    }
}

impl TryFrom<&str> for TopicFilter {
    type Error = TopicFilterParseError;

    fn try_from(topic: &str) -> Result<Self, Self::Error> {
        TopicFilter::try_from(topic.to_string())
    }
}

impl TryFrom<Bytes> for TopicFilter {
    type Error = TopicFilterParseError;

    fn try_from(topic: Bytes) -> Result<Self, Self::Error> {
        let topic = String::from_utf8(topic.to_vec())?;
        TopicFilter::try_from(topic)
    }
}

impl From<Topic> for TopicFilter {
    fn from(topic: Topic) -> Self {
        TopicFilter(topic.0)
    }
}

impl AsRef<str> for TopicFilter {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl TryFrom<TopicFilter> for Topic {
    type Error = TopicParseError;

    fn try_from(topic: TopicFilter) -> Result<Self, Self::Error> {
        Topic::try_from(topic.0)
    }
}

impl TopicFilter {
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    pub fn is_wildcard(&self) -> bool {
        self.0
            .contains(|x: char| x == SINGLE_LEVEL_WILDCARD || x == MULTI_LEVEL_WILDCARD)
    }
}

impl std::fmt::Display for TopicFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_filter_parse_empty_topic() {
        let empty = Err(TopicFilterParseError::EmptyTopicFilter);
        assert_eq!(TopicFilter::try_from(""), empty);
    }

    #[test]
    fn test_topic_filter_parse_length() {
        let just_right_topic = "a".repeat(MAX_TOPIC_LEN_BYTES);
        assert!(TopicFilter::try_from(just_right_topic).is_ok());

        let too_long_topic = "a".repeat(MAX_TOPIC_LEN_BYTES + 1);
        assert_eq!(
            TopicFilter::try_from(too_long_topic),
            Err(TopicFilterParseError::TopicFilterTooLong)
        );
    }

    #[test]
    fn test_topic_filter_parse_concrete() -> Result<(), TopicFilterParseError> {
        assert!(!TopicFilter::try_from("/")?.is_wildcard());
        assert!(!TopicFilter::try_from("a")?.is_wildcard());
        assert!(!TopicFilter::try_from("home/kitchen")?.is_wildcard());
        assert!(!TopicFilter::try_from("home/kitchen/temperature")?.is_wildcard());
        assert!(!TopicFilter::try_from("home/kitchen/temperature/celsius")?.is_wildcard());
        Ok(())
    }

    #[test]
    fn test_topic_filter_parse_single_level_wildcard() -> Result<(), TopicFilterParseError> {
        assert!(TopicFilter::try_from("+")?.is_wildcard());
        assert!(TopicFilter::try_from("+/")?.is_wildcard());
        assert!(TopicFilter::try_from("sport/+")?.is_wildcard());
        assert!(TopicFilter::try_from("/+")?.is_wildcard());
        Ok(())
    }

    #[test]
    fn test_topic_filter_parse_multi_level_wildcard() -> Result<(), TopicFilterParseError> {
        assert!(TopicFilter::try_from("#")?.is_wildcard());
        assert!(TopicFilter::try_from("/#")?.is_wildcard());
        assert!(TopicFilter::try_from("sport/#")?.is_wildcard());
        assert!(TopicFilter::try_from("home/kitchen/temperature/#")?.is_wildcard());
        Ok(())
    }

    #[test]
    fn test_topic_filter_parse_invalid_filters() -> Result<(), TopicFilterParseError> {
        assert_eq!(
            TopicFilter::try_from("sport/#/stats"),
            Err(TopicFilterParseError::MultilevelWildcardNotAtEnd)
        );
        assert_eq!(
            TopicFilter::try_from("sport/#/stats#"),
            Err(TopicFilterParseError::InvalidWildcardLevel)
        );
        assert_eq!(
            TopicFilter::try_from("sport#/stats#"),
            Err(TopicFilterParseError::InvalidWildcardLevel)
        );
        assert_eq!(
            TopicFilter::try_from("sport/tennis#"),
            Err(TopicFilterParseError::InvalidWildcardLevel)
        );
        assert_eq!(
            TopicFilter::try_from("sport/++"),
            Err(TopicFilterParseError::InvalidWildcardLevel)
        );
        Ok(())
    }

    #[test]
    fn test_topic_name_failure() -> Result<(), TopicParseError> {
        let err = Err(TopicParseError::WildcardOrNullInTopic);
        assert_eq!(Topic::try_from("#"), err);
        assert_eq!(Topic::try_from("+"), err);
        assert_eq!(Topic::try_from("\0"), err);
        assert_eq!(Topic::try_from("/multi/level/#"), err);
        assert_eq!(Topic::try_from("/single/level/+"), err);
        assert_eq!(Topic::try_from("/null/byte/\0"), err);
        Ok(())
    }
}
