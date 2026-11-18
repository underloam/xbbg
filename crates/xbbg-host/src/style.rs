//! How a binding words and validates the messages of the shared planners.
//!
//! The request planner holds the rules (what is invalid, in which order, under
//! which element). [`Style`] only chooses how strict a binding is about
//! enum-like values: JavaScript accepts the documented lower-case values only;
//! Python accepts the case- and whitespace-insensitive spellings the engine
//! itself parses.

/// Strictness of one binding family. Java and Node use [`Style::JavaScript`],
/// the default of every planner entry point.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Style {
    /// Exact enum values.
    #[default]
    JavaScript,
    /// The engine's trimmed, case-insensitive enum parsing.
    Python,
}

impl Style {
    /// Whether enum-like values may differ in case and surrounding whitespace.
    pub(crate) fn lenient_values(self) -> bool {
        self == Self::Python
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn javascript_is_the_default_and_only_python_is_lenient() {
        assert_eq!(Style::default(), Style::JavaScript);
        assert!(Style::Python.lenient_values());
        assert!(!Style::JavaScript.lenient_values());
    }
}
