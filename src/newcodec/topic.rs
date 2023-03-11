use super::{
    MAX_TOPIC_LEN_BYTES, MULTI_LEVEL_WILDCARD, MULTI_LEVEL_WILDCARD_STR,
    SHARED_SUBSCRIPTION_PREFIX, SINGLE_LEVEL_WILDCARD, SINGLE_LEVEL_WILDCARD_STR, TOPIC_SEPARATOR,
};
use std::str::FromStr;

/// A filter for subscribers to indicate which topics they want
/// to receive messages from. Can contain wildcards.
/// Shared topic filter example: $share/group_name_a/home/kitchen/temperature
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicFilter {
    Concrete,
    Wildcard,
    SharedConcrete,
    SharedWildcard,
}

/// A topic name publishers use when sending MQTT messages.
/// Cannot contain wildcards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topic {
    topic_name: String,
    level_count: u32,
}

impl Topic {
    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum TopicLevel<'a> {
    Concrete(&'a str),
    SingleLevelWildcard,
    MultiLevelWildcard,
}

#[derive(Debug, PartialEq, Eq)]
pub enum TopicParseError {
    EmptyTopic,
    TopicTooLong,
    MultilevelWildcardNotAtEnd,
    InvalidWildcardLevel,
    InvalidSharedGroupName,
    EmptySharedGroupName,
    WildcardOrNullInTopic,
}

impl std::fmt::Display for Topic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.topic_name)
    }
}

/// If Ok, returns (level_count, contains_wildcards).
fn process_filter(filter: &str) -> Result<(u32, bool), TopicParseError> {
    let mut level_count = 0;
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

        level_count += 1;
    }

    Ok((level_count, contains_wildcards))
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

        let mut shared_group = None;

        if let Some(filter_rest) = filter.strip_prefix(SHARED_SUBSCRIPTION_PREFIX) {
            if filter_rest.is_empty() {
                return Err(TopicParseError::EmptySharedGroupName);
            }

            if let Some(slash_pos) = filter_rest.find(TOPIC_SEPARATOR) {
                let shared_name = &filter_rest[0..slash_pos];

                // slash_pos+1 is safe here, we've already validated the string
                // has a nonzero length.
                let shared_filter = &filter_rest[(slash_pos + 1)..];

                if shared_name.is_empty() {
                    return Err(TopicParseError::EmptySharedGroupName);
                }

                if shared_name
                    .contains(|x: char| x == SINGLE_LEVEL_WILDCARD || x == MULTI_LEVEL_WILDCARD)
                {
                    return Err(TopicParseError::InvalidSharedGroupName);
                }

                if shared_filter.is_empty() {
                    return Err(TopicParseError::EmptyTopic);
                }

                shared_group = Some((shared_name, shared_filter))
            } else {
                return Err(TopicParseError::EmptyTopic);
            }
        }

        let topic_filter = if let Some((group_name, shared_filter)) = shared_group {
            let (level_count, contains_wildcards) = process_filter(shared_filter)?;

            if contains_wildcards {
                TopicFilter::SharedWildcard
            } else {
                TopicFilter::SharedConcrete
            }
        } else {
            let (level_count, contains_wildcards) = process_filter(filter)?;

            if contains_wildcards {
                TopicFilter::Wildcard
            } else {
                TopicFilter::Concrete
            }
        };

        Ok(topic_filter)
    }
}

impl FromStr for Topic {
    type Err = TopicParseError;

    fn from_str(topic: &str) -> Result<Self, Self::Err> {
        // TODO - Consider disallowing leading $ characters

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

        let level_count = topic.split(TOPIC_SEPARATOR).count() as u32;

        let topic = Topic {
            topic_name: topic.to_string(),
            level_count,
        };

        Ok(topic)
    }
}

pub struct TopicLevels<'a> {
    levels_iter: std::str::Split<'a, char>,
}

impl<'a> Iterator for TopicLevels<'a> {
    type Item = TopicLevel<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.levels_iter.next() {
            Some(MULTI_LEVEL_WILDCARD_STR) => Some(TopicLevel::MultiLevelWildcard),
            Some(SINGLE_LEVEL_WILDCARD_STR) => Some(TopicLevel::SingleLevelWildcard),
            Some(level) => Some(TopicLevel::Concrete(level)),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Topic, TopicFilter, TopicParseError, MAX_TOPIC_LEN_BYTES};

    #[test]
    fn test_topic_filter_parse_empty_topic() {
        assert_eq!(
            "".parse::<TopicFilter>().unwrap_err(),
            TopicParseError::EmptyTopic
        );
    }

    #[test]
    fn test_topic_filter_parse_length() {
        let just_right_topic = "a".repeat(MAX_TOPIC_LEN_BYTES);
        assert!(just_right_topic.parse::<TopicFilter>().is_ok());

        let too_long_topic = "a".repeat(MAX_TOPIC_LEN_BYTES + 1);
        assert_eq!(
            too_long_topic.parse::<TopicFilter>().unwrap_err(),
            TopicParseError::TopicTooLong
        );
    }

    #[test]
    fn test_topic_filter_parse_concrete() {
        assert_eq!("/".parse::<TopicFilter>().unwrap(), TopicFilter::Concrete);

        assert_eq!("a".parse::<TopicFilter>().unwrap(), TopicFilter::Concrete);

        // $SYS topics can be subscribed to, but can't be published
        assert_eq!(
            "home/kitchen".parse::<TopicFilter>().unwrap(),
            TopicFilter::Concrete
        );

        assert_eq!(
            "home/kitchen/temperature".parse::<TopicFilter>().unwrap(),
            TopicFilter::Concrete
        );

        assert_eq!(
            "home/kitchen/temperature/celsius"
                .parse::<TopicFilter>()
                .unwrap(),
            TopicFilter::Concrete
        );
    }

    #[test]
    fn test_topic_filter_parse_single_level_wildcard() {
        assert_eq!("+".parse::<TopicFilter>().unwrap(), TopicFilter::Wildcard);

        assert_eq!("+/".parse::<TopicFilter>().unwrap(), TopicFilter::Wildcard);

        assert_eq!(
            "sport/+".parse::<TopicFilter>().unwrap(),
            TopicFilter::Wildcard
        );

        assert_eq!("/+".parse::<TopicFilter>().unwrap(), TopicFilter::Wildcard);
    }

    #[test]
    fn test_topic_filter_parse_multi_level_wildcard() {
        assert_eq!("#".parse::<TopicFilter>().unwrap(), TopicFilter::Wildcard);

        assert_eq!(
            "#/".parse::<TopicFilter>().unwrap_err(),
            TopicParseError::MultilevelWildcardNotAtEnd
        );

        assert_eq!("/#".parse::<TopicFilter>().unwrap(), TopicFilter::Wildcard);

        assert_eq!(
            "sport/#".parse::<TopicFilter>().unwrap(),
            TopicFilter::Wildcard
        );

        assert_eq!(
            "home/kitchen/temperature/#".parse::<TopicFilter>().unwrap(),
            TopicFilter::Wildcard
        );
    }

    #[test]
    fn test_topic_filter_parse_shared_subscription_concrete() {
        assert_eq!(
            "$share/group_a/home".parse::<TopicFilter>().unwrap(),
            TopicFilter::SharedConcrete
        );

        assert_eq!(
            "$share/group_a/home/kitchen/temperature"
                .parse::<TopicFilter>()
                .unwrap(),
            TopicFilter::SharedConcrete
        );

        assert_eq!(
            "$share/group_a//".parse::<TopicFilter>().unwrap(),
            TopicFilter::SharedConcrete
        );
    }

    #[test]
    fn test_topic_filter_parse_shared_subscription_wildcard() {
        assert_eq!(
            "$share/group_b/#".parse::<TopicFilter>().unwrap(),
            TopicFilter::SharedWildcard
        );

        assert_eq!(
            "$share/group_b/+".parse::<TopicFilter>().unwrap(),
            TopicFilter::SharedWildcard
        );

        assert_eq!(
            "$share/group_b/+/temperature"
                .parse::<TopicFilter>()
                .unwrap(),
            TopicFilter::SharedWildcard
        );

        assert_eq!(
            "$share/group_c/+/temperature/+/meta"
                .parse::<TopicFilter>()
                .unwrap(),
            TopicFilter::SharedWildcard
        );
    }

    #[test]
    fn test_topic_filter_parse_invalid_shared_subscription() {
        assert_eq!(
            "$share/".parse::<TopicFilter>().unwrap_err(),
            TopicParseError::EmptySharedGroupName
        );
        assert_eq!(
            "$share/a".parse::<TopicFilter>().unwrap_err(),
            TopicParseError::EmptyTopic
        );
        assert_eq!(
            "$share/a/".parse::<TopicFilter>().unwrap_err(),
            TopicParseError::EmptyTopic
        );
        assert_eq!(
            "$share//".parse::<TopicFilter>().unwrap_err(),
            TopicParseError::EmptySharedGroupName
        );
        assert_eq!(
            "$share///".parse::<TopicFilter>().unwrap_err(),
            TopicParseError::EmptySharedGroupName
        );

        assert_eq!(
            "$share/invalid_group#/#"
                .parse::<TopicFilter>()
                .unwrap_err(),
            TopicParseError::InvalidSharedGroupName
        );
    }

    #[test]
    fn test_topic_filter_parse_sys_prefix() {
        assert_eq!(
            "$SYS/stats".parse::<TopicFilter>().unwrap(),
            TopicFilter::Concrete
        );

        assert_eq!(
            "/$SYS/stats".parse::<TopicFilter>().unwrap(),
            TopicFilter::Concrete
        );

        assert_eq!(
            "$SYS/+".parse::<TopicFilter>().unwrap(),
            TopicFilter::Wildcard
        );

        assert_eq!(
            "$SYS/#".parse::<TopicFilter>().unwrap(),
            TopicFilter::Wildcard
        );
    }

    #[test]
    fn test_topic_filter_parse_invalid_filters() {
        assert_eq!(
            "sport/#/stats".parse::<TopicFilter>().unwrap_err(),
            TopicParseError::MultilevelWildcardNotAtEnd
        );
        assert_eq!(
            "sport/#/stats#".parse::<TopicFilter>().unwrap_err(),
            TopicParseError::InvalidWildcardLevel
        );
        assert_eq!(
            "sport#/stats#".parse::<TopicFilter>().unwrap_err(),
            TopicParseError::InvalidWildcardLevel
        );
        assert_eq!(
            "sport/tennis#".parse::<TopicFilter>().unwrap_err(),
            TopicParseError::InvalidWildcardLevel
        );
        assert_eq!(
            "sport/++".parse::<TopicFilter>().unwrap_err(),
            TopicParseError::InvalidWildcardLevel
        );
    }

    #[test]
    fn test_topic_name_success() {
        assert_eq!(
            "/".parse::<Topic>().unwrap(),
            Topic {
                topic_name: "/".to_string(),
                level_count: 2
            }
        );

        assert_eq!(
            "Accounts payable".parse::<Topic>().unwrap(),
            Topic {
                topic_name: "Accounts payable".to_string(),
                level_count: 1
            }
        );

        assert_eq!(
            "home/kitchen".parse::<Topic>().unwrap(),
            Topic {
                topic_name: "home/kitchen".to_string(),
                level_count: 2
            }
        );

        assert_eq!(
            "home/kitchen/temperature".parse::<Topic>().unwrap(),
            Topic {
                topic_name: "home/kitchen/temperature".to_string(),
                level_count: 3
            }
        );
    }

    #[test]
    fn test_topic_name_failure() {
        assert_eq!(
            "#".parse::<Topic>().unwrap_err(),
            TopicParseError::WildcardOrNullInTopic,
        );

        assert_eq!(
            "+".parse::<Topic>().unwrap_err(),
            TopicParseError::WildcardOrNullInTopic,
        );

        assert_eq!(
            "\0".parse::<Topic>().unwrap_err(),
            TopicParseError::WildcardOrNullInTopic,
        );

        assert_eq!(
            "/multi/level/#".parse::<Topic>().unwrap_err(),
            TopicParseError::WildcardOrNullInTopic,
        );

        assert_eq!(
            "/single/level/+".parse::<Topic>().unwrap_err(),
            TopicParseError::WildcardOrNullInTopic,
        );

        assert_eq!(
            "/null/byte/\0".parse::<Topic>().unwrap_err(),
            TopicParseError::WildcardOrNullInTopic,
        );
    }
}
