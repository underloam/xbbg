//! How a binding words the messages of the shared planners.
//!
//! The request and subscription planners hold the rules (what is invalid, in
//! which order, under which element). [`Style`] only chooses how a binding
//! spells the result: JavaScript names members in camelCase and accepts the
//! documented lower-case values only; Python names them by its snake_case
//! parameters, spells literals the Python way, and accepts the case- and
//! whitespace-insensitive spellings the engine itself parses.

/// Wording and strictness of one binding family. Java and Node use
/// [`Style::JavaScript`], the default of every planner entry point.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Style {
    /// camelCase member names, JavaScript literals (`true`), exact enum values.
    #[default]
    JavaScript,
    /// snake_case parameter names, Python literals (`True`), and the engine's
    /// trimmed, case-insensitive enum parsing.
    Python,
}

impl Style {
    /// The member name as this style writes it.
    pub(crate) fn name(self, javascript: &'static str, python: &'static str) -> &'static str {
        match self {
            Self::JavaScript => javascript,
            Self::Python => python,
        }
    }

    /// Whether enum-like values may differ in case and surrounding whitespace.
    pub(crate) fn lenient_values(self) -> bool {
        self == Self::Python
    }

    /// A boolean literal in a message.
    pub(crate) fn flag(self, value: bool) -> &'static str {
        match (self, value) {
            (Self::JavaScript, true) => "true",
            (Self::JavaScript, false) => "false",
            (Self::Python, true) => "True",
            (Self::Python, false) => "False",
        }
    }
}

/// Python's `repr()` of a `str`, for messages that quote the offending value.
///
/// Matches CPython for every printable character: single quotes unless the text
/// holds a single quote and no double quote, backslash escapes for the quote,
/// `\`, `\n`, `\r`, `\t`, and `\xNN` for other ASCII controls. Non-ASCII
/// characters pass through as written, which differs from CPython only for the
/// rare ones it would escape as non-printable.
pub(crate) fn python_str_repr(text: &str) -> String {
    let quote = if text.contains('\'') && !text.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut out = String::with_capacity(text.len() + 2);
    out.push(quote);
    for c in text.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            c if c.is_ascii_control() => {
                out.push_str(&format!("\\x{:02x}", u32::from(c)));
            }
            c => out.push(c),
        }
    }
    out.push(quote);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repr_quotes_like_python() {
        assert_eq!(python_str_repr("fast"), "'fast'");
        assert_eq!(python_str_repr(""), "''");
        assert_eq!(python_str_repr("it's"), "\"it's\"");
        assert_eq!(python_str_repr("it's \"x\""), "'it\\'s \"x\"'");
        assert_eq!(python_str_repr("a\\b\n\t\r"), "'a\\\\b\\n\\t\\r'");
        assert_eq!(python_str_repr("\u{1}\u{7f}"), "'\\x01\\x7f'");
        assert_eq!(python_str_repr("caf\u{e9}"), "'caf\u{e9}'");
    }

    #[test]
    fn styles_choose_names_and_literals() {
        assert_eq!(Style::default(), Style::JavaScript);
        assert_eq!(
            Style::JavaScript.name("onDelayed", "on_delayed"),
            "onDelayed"
        );
        assert_eq!(Style::Python.name("onDelayed", "on_delayed"), "on_delayed");
        assert_eq!(Style::JavaScript.flag(false), "false");
        assert_eq!(Style::Python.flag(true), "True");
        assert!(Style::Python.lenient_values());
        assert!(!Style::JavaScript.lenient_values());
    }
}
