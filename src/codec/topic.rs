use bytes::Bytes;
use std::str::FromStr;

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

impl std::fmt::Display for Topic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicFilter {
    Concrete,
    Wildcard,
}

impl FromStr for TopicFilter {
    type Err = TopicParseError;

    fn from_str(filter: &str) -> Result<Self, Self::Err> {
        // Filters and topics cannot be empty
        if filter.is_empty() {
            return Err(TopicParseError::EmptyTopic);
        }

        // Assert no null character U+0000
        if filter.contains('\0') {
            return Err(TopicParseError::WildcardOrNullInTopic);
        }

        // Filters cannot exceed the byte length in the MQTT spec
        if filter.len() > MAX_TOPIC_LEN_BYTES {
            return Err(TopicParseError::TopicTooLong);
        }

        // Multi-level wildcards can only be at the end of the topic
        if let Some(pos) = filter.rfind(MULTI_LEVEL_WILDCARD) {
            if pos != filter.len() - 1 {
                return Err(TopicParseError::MultilevelWildcardNotAtEnd);
            }
        }

        let mut contains_wildcards = false;
        for level in filter.split(TOPIC_SEPARATOR) {
            let level_contains_wildcard =
                level.contains(|x: char| x == SINGLE_LEVEL_WILDCARD || x == MULTI_LEVEL_WILDCARD);
            if level_contains_wildcard {
                // Any wildcards on a particular level must be specified on their own
                if level.len() > 1 {
                    return Err(TopicParseError::InvalidWildcardLevel);
                }

                contains_wildcards = true;
            }
        }

        if contains_wildcards {
            Ok(TopicFilter::Wildcard)
        } else {
            Ok(TopicFilter::Concrete)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_filter_parse_empty_topic() {
        assert_eq!("".parse::<TopicFilter>(), Err(TopicParseError::EmptyTopic));
    }

    #[test]
    fn test_topic_filter_parse_length() {
        let just_right_topic = "a".repeat(MAX_TOPIC_LEN_BYTES);
        assert!(just_right_topic.parse::<TopicFilter>().is_ok());

        let too_long_topic = "a".repeat(MAX_TOPIC_LEN_BYTES + 1);
        assert_eq!(
            too_long_topic.parse::<TopicFilter>(),
            Err(TopicParseError::TopicTooLong)
        );
    }

    #[test]
    fn test_topic_filter_parse_concrete() -> Result<(), TopicParseError> {
        assert_eq!("/".parse::<TopicFilter>()?, TopicFilter::Concrete);
        assert_eq!("a".parse::<TopicFilter>()?, TopicFilter::Concrete);

        // $SYS topics can be subscribed to, but can't be published
        assert_eq!(
            "home/kitchen".parse::<TopicFilter>()?,
            TopicFilter::Concrete
        );

        assert_eq!(
            "home/kitchen/temperature".parse::<TopicFilter>()?,
            TopicFilter::Concrete
        );
        assert_eq!(
            "home/kitchen/temperature/celsius".parse::<TopicFilter>()?,
            TopicFilter::Concrete
        );

        Ok(())
    }

    #[test]
    fn test_topic_filter_parse_single_level_wildcard() -> Result<(), TopicParseError> {
        assert_eq!("+".parse::<TopicFilter>()?, TopicFilter::Wildcard);
        assert_eq!("+/".parse::<TopicFilter>()?, TopicFilter::Wildcard);
        assert_eq!("sport/+".parse::<TopicFilter>()?, TopicFilter::Wildcard);
        assert_eq!("/+".parse::<TopicFilter>()?, TopicFilter::Wildcard);
        Ok(())
    }

    #[test]
    fn test_topic_filter_parse_multi_level_wildcard() -> Result<(), TopicParseError> {
        assert_eq!("#".parse::<TopicFilter>()?, TopicFilter::Wildcard);
        assert_eq!(
            "#/".parse::<TopicFilter>(),
            Err(TopicParseError::MultilevelWildcardNotAtEnd)
        );
        assert_eq!("/#".parse::<TopicFilter>()?, TopicFilter::Wildcard);
        assert_eq!("sport/#".parse::<TopicFilter>()?, TopicFilter::Wildcard);
        assert_eq!(
            "home/kitchen/temperature/#".parse::<TopicFilter>()?,
            TopicFilter::Wildcard
        );
        Ok(())
    }

    #[test]
    fn test_topic_filter_parse_invalid_filters() -> Result<(), TopicParseError> {
        assert_eq!(
            "sport/#/stats".parse::<TopicFilter>(),
            Err(TopicParseError::MultilevelWildcardNotAtEnd)
        );
        assert_eq!(
            "sport/#/stats#".parse::<TopicFilter>(),
            Err(TopicParseError::InvalidWildcardLevel)
        );
        assert_eq!(
            "sport#/stats#".parse::<TopicFilter>(),
            Err(TopicParseError::InvalidWildcardLevel)
        );
        assert_eq!(
            "sport/tennis#".parse::<TopicFilter>(),
            Err(TopicParseError::InvalidWildcardLevel)
        );
        assert_eq!(
            "sport/++".parse::<TopicFilter>(),
            Err(TopicParseError::InvalidWildcardLevel)
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
