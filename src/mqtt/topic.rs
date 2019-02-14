use std::collections::HashSet;
use std::hash::*;
use std::hash::Hasher;

use mqtt::*;

impl PartialEq for TopicNode {
    fn eq(&self, other: &TopicNode) -> bool {
        self.path == other.path
    }
}

impl Eq for TopicNode {}

impl Hash for TopicNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.path.hash(state);
    }
}

struct TopicNode {
    path: String,
    leafs: HashSet<TopicNode>,
    clients: HashSet<ClientId>,
}

impl TopicNode {
    fn new() -> Self {
        TopicNode {
            path: "".to_owned(),
            leafs: HashSet::new(),
            clients: HashSet::new(),
        }
    }

    fn register(&mut self, topic: Topic, client: ClientId) {
        let mut splitter = topic.splitn(2, "\\");
        match splitter.next() {
            Some(path) => {

            }
            None => println!("check clients")
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registration() {
        let mut root = TopicNode::new();
        root.register("\\a\\b\\c\\d".to_string(), "abc".to_string());
    }
}