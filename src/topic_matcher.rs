pub fn is_match(topic_filter: &str, topic_name: &str) -> bool {
    let mut topic_itr = topic_name.split('/');
    let mut filter_itr = topic_filter.split('/');

    let first_filter_item = filter_itr.next().unwrap();
    let first_topic_item = topic_itr.next().unwrap();

    if first_topic_item.starts_with('$') {
        if first_topic_item != first_filter_item {
            return false;
        }
    } else {
        match first_filter_item {
            // Matches the whole topic
            "#" => return true,
            "+" => {}
            _ => {
                if first_topic_item != first_filter_item {
                    return false;
                }
            }
        }
    }

    loop {
        match (filter_itr.next(), topic_itr.next()) {
            (Some(ft), Some(tn)) => match ft {
                "#" => break,
                "+" => {}
                _ => {
                    if ft != tn {
                        return false;
                    }
                }
            },
            (Some(ft), None) => {
                if ft != "#" {
                    return false;
                } else {
                    break;
                }
            }
            (None, Some(..)) => return false,
            (None, None) => break,
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_filter_matcher() {
        assert!(is_match("sport/#", "sport"));

        assert!(is_match("#", "sport"));
        assert!(is_match("#", "/"));
        assert!(is_match("#", "abc/def"));
        assert!(!is_match("#", "$SYS"));
        assert!(!is_match("#", "$SYS/abc"));

        assert!(!is_match("+/monitor/Clients", "$SYS/monitor/Clients"));

        assert!(is_match("$SYS/#", "$SYS/monitor/Clients"));
        assert!(is_match("$SYS/#", "$SYS"));

        assert!(is_match("$SYS/monitor/+", "$SYS/monitor/Clients"));
    }
}
