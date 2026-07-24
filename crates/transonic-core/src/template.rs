//! Pipeline template parsing and application.
//!
//! Pipeline bin specs may embed placeholders of the form
//! `$(pipeline_parameters.<key>)` or `$(pipeline_parameters.<key>,<default>)`.
//! When a job is submitted, per-channel parameter maps are substituted into
//! the template to produce the final `gst-launch`-style pipeline fragment.
//!
//! Keys must start with an ASCII letter and may contain ASCII letters,
//! digits, `_` and `-`.  Values are restricted to a small set of scalar
//! serializations (boolean / double / signed / unsigned integers) so that a
//! caller cannot inject arbitrary pipeline syntax.

use std::collections::BTreeMap;
use std::fmt;

use crate::error::TemplateError;

/// A validated pipeline template key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TemplateKey(String);

impl TemplateKey {
    /// Validates and wraps a template key.
    pub fn new(s: impl Into<String>) -> Result<Self, TemplateError> {
        let s = s.into();
        let mut chars = s.chars();
        let valid_prefix = chars.next().is_some_and(|c| c.is_ascii_alphabetic());
        let valid_suffix = chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
        if !(valid_prefix && valid_suffix) {
            return Err(TemplateError(format!(
                "'{s}' is not a valid pipeline template key"
            )));
        }
        Ok(TemplateKey(s))
    }

    /// Returns the key as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TemplateKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A validated pipeline template value.
///
/// Template values can be embedded anywhere in the pipeline.  To limit what a
/// caller can inject, the value must parse as one of a few scalar formats
/// (the original used `gst_value_deserialize` against BOOLEAN / DOUBLE /
/// INT64 / UINT64).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TemplateValue(String);

impl TemplateValue {
    /// Validates and wraps a template value.
    pub fn new(s: impl Into<String>) -> Result<Self, TemplateError> {
        let s = s.into();
        if s.is_empty() {
            return Err(TemplateError(
                "the empty string is not a valid pipeline template value".into(),
            ));
        }
        let is_boolean = matches!(
            s.to_ascii_lowercase().as_str(),
            "true" | "false" | "t" | "f" | "yes" | "no" | "0" | "1"
        );
        let is_scalar = s.parse::<f64>().is_ok()
            || s.parse::<i64>().is_ok()
            || s.parse::<u64>().is_ok()
            || is_boolean;
        if !is_scalar {
            return Err(TemplateError(format!(
                "'{s}' is not a valid pipeline template value"
            )));
        }
        Ok(TemplateValue(s))
    }

    /// Returns the value as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TemplateValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A placeholder followed by literal text that is rendered "as is".
#[derive(Debug, Clone)]
struct TemplateRegion {
    key: TemplateKey,
    default_value: Option<TemplateValue>,
    text: String,
}

/// A parsed pipeline template.
#[derive(Debug, Clone)]
pub struct PipelineTemplate {
    prefix: String,
    regions: Vec<TemplateRegion>,
}

const PLACEHOLDER_PREFIX: &str = "$(pipeline_parameters.";

impl PipelineTemplate {
    /// Parses the given string as a pipeline template.
    pub fn parse(spec: &str) -> Result<Self, TemplateError> {
        let mut rest = spec;
        let mut prefix = String::new();
        let mut regions = Vec::new();

        // Read text up to the next '$'.
        fn read_text(s: &str) -> (&str, &str) {
            match s.find('$') {
                Some(pos) => (&s[..pos], &s[pos..]),
                None => (s, ""),
            }
        }

        let (text, tail) = read_text(rest);
        prefix.push_str(text);
        rest = tail;

        while !rest.is_empty() {
            let Some(after) = rest.strip_prefix(PLACEHOLDER_PREFIX) else {
                return Err(TemplateError(format!(
                    "'{spec}' is not a valid pipeline template: expected string \
                     '$(pipeline_parameters.' not found at position {}",
                    spec.len() - rest.len()
                )));
            };

            // Parse the key.
            let key_len = {
                let mut chars = after.char_indices();
                match chars.next() {
                    Some((_, c)) if c.is_ascii_alphabetic() => {}
                    _ => {
                        return Err(TemplateError(format!(
                            "'{spec}' is not a valid pipeline template: pipeline \
                             template keys must begin with an ASCII alphabet character"
                        )));
                    }
                }
                after
                    .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '-'))
                    .unwrap_or(after.len())
            };
            let key = TemplateKey::new(&after[..key_len]).map_err(|e| {
                TemplateError(format!("'{spec}' is not a valid pipeline template: {e}"))
            })?;
            let mut cursor = &after[key_len..];

            // Parse the optional default value.
            let mut default_value = None;
            if let Some(after_comma) = cursor.strip_prefix(',') {
                let end = after_comma.find(')').unwrap_or(after_comma.len());
                default_value = Some(TemplateValue::new(&after_comma[..end]).map_err(|e| {
                    TemplateError(format!("'{spec}' is not a valid pipeline template: {e}"))
                })?);
                cursor = &after_comma[end..];
            }

            let Some(after_paren) = cursor.strip_prefix(')') else {
                return Err(TemplateError(format!(
                    "'{spec}' is not a valid pipeline template: expected ')' not \
                     found at position {}",
                    spec.len() - cursor.len()
                )));
            };

            let (text, tail) = read_text(after_paren);
            regions.push(TemplateRegion {
                key,
                default_value,
                text: text.to_owned(),
            });
            rest = tail;
        }

        Ok(PipelineTemplate { prefix, regions })
    }

    /// Applies the given value map to the template and returns the rendered
    /// pipeline fragment.
    pub fn apply(
        &self,
        values: &BTreeMap<TemplateKey, TemplateValue>,
    ) -> Result<String, TemplateError> {
        let mut result = self.prefix.clone();
        for region in &self.regions {
            match values.get(&region.key).or(region.default_value.as_ref()) {
                Some(value) => result.push_str(value.as_str()),
                None => {
                    return Err(TemplateError(format!(
                        "required pipeline template key '{}' not found in value map",
                        region.key
                    )));
                }
            }
            result.push_str(&region.text);
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn values(pairs: &[(&str, &str)]) -> BTreeMap<TemplateKey, TemplateValue> {
        pairs
            .iter()
            .map(|(k, v)| {
                (
                    TemplateKey::new(*k).unwrap(),
                    TemplateValue::new(*v).unwrap(),
                )
            })
            .collect()
    }

    #[test]
    fn plain_template_passes_through() {
        let template = PipelineTemplate::parse("decodebin ! audioconvert ! queue ! c.").unwrap();
        assert_eq!(
            template.apply(&values(&[])).unwrap(),
            "decodebin ! audioconvert ! queue ! c."
        );
    }

    #[test]
    fn placeholders_substitute_supplied_values() {
        let template = PipelineTemplate::parse(
            "mixer volume=$(pipeline_parameters.volume,1) fadein=$(pipeline_parameters.fadein,0)",
        )
        .unwrap();
        assert_eq!(
            template.apply(&values(&[("volume", "2.5")])).unwrap(),
            "mixer volume=2.5 fadein=0"
        );
    }

    #[test]
    fn missing_required_key_is_an_error() {
        let template = PipelineTemplate::parse("volume=$(pipeline_parameters.volume)").unwrap();
        assert!(template.apply(&values(&[])).is_err());
    }

    #[test]
    fn injection_values_are_rejected() {
        assert!(TemplateValue::new("1 ! filesink location=/etc/passwd").is_err());
        assert!(TemplateValue::new("").is_err());
        assert!(TemplateValue::new("2.5").is_ok());
        assert!(TemplateValue::new("true").is_ok());
        assert!(TemplateValue::new("-3").is_ok());
    }

    #[test]
    fn malformed_placeholder_is_rejected() {
        assert!(PipelineTemplate::parse("volume=$(other.key)").is_err());
        assert!(PipelineTemplate::parse("volume=$(pipeline_parameters.9bad)").is_err());
        assert!(PipelineTemplate::parse("volume=$(pipeline_parameters.key").is_err());
    }

    #[test]
    fn invalid_default_values_are_rejected() {
        assert!(PipelineTemplate::parse("v=$(pipeline_parameters.k,not a number)").is_err());
        assert!(PipelineTemplate::parse("v=$(pipeline_parameters.k,)").is_err());
        assert!(PipelineTemplate::parse("v=$(pipeline_parameters.k,1 ! filesink)").is_err());
    }

    #[test]
    fn adjacent_placeholders_render_in_order() {
        let template =
            PipelineTemplate::parse("$(pipeline_parameters.a,1)$(pipeline_parameters.b,2) tail")
                .unwrap();
        assert_eq!(template.apply(&values(&[("b", "9")])).unwrap(), "19 tail");
    }

    #[test]
    fn key_charset_is_validated() {
        assert!(TemplateKey::new("a1_x-y").is_ok());
        assert!(TemplateKey::new("1abc").is_err());
        assert!(TemplateKey::new("").is_err());
        assert!(TemplateKey::new("has space").is_err());
    }
}
