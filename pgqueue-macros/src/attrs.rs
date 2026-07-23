//! Parsing of `#[pgqueue::job(...)]` attribute arguments.

use std::collections::HashSet;

use proc_macro2::TokenStream;
use syn::punctuated::Punctuated;
use syn::{Expr, ExprLit, ExprUnary, Lit, Meta, Token, UnOp};

/// Parsed attribute arguments; every field is optional and overrides
/// `JobConfig::default()` in the generated `config()`.
#[derive(Debug, Default)]
pub(crate) struct JobAttrs {
    /// `name = "custom"` — registry/database name (default: the fn name).
    pub name: Option<String>,
    /// `max_attempts = 3` — total attempts allowed.
    pub max_attempts: Option<u32>,
    /// `timeout_ms = 30_000` — per-attempt limit; zero disables the timeout.
    pub timeout_ms: Option<Option<u64>>,
    /// `heartbeat_ms = 10_000` — required touch() interval.
    pub heartbeat_ms: Option<u64>,
    /// `ttl_ms = 3_600_000` — result retention; zero deletes immediately.
    pub ttl_ms: Option<Ttl>,
    /// `retry_delay_ms = 500` — base retry delay.
    pub retry_delay_ms: Option<u64>,
    /// `backoff` (uncapped) or `backoff_max_ms = 60_000` (capped).
    pub backoff_max_ms: Option<Option<u64>>,
    /// `priority = -1` — dequeue priority (lower first).
    pub priority: Option<i16>,
    /// `revision = 2` — durable cron definition revision.
    pub revision: Option<u64>,
}

/// Result retention as written in the attribute.
#[derive(Debug)]
pub(crate) enum Ttl {
    ForMs(u64),
    Delete,
}

impl JobAttrs {
    pub(crate) fn parse(tokens: TokenStream) -> syn::Result<Self> {
        let mut attrs = JobAttrs::default();
        if tokens.is_empty() {
            return Ok(attrs);
        }
        let metas =
            syn::parse::Parser::parse2(Punctuated::<Meta, Token![,]>::parse_terminated, tokens)?;
        let mut seen = HashSet::new();

        for meta in metas {
            match &meta {
                Meta::Path(path) if path.is_ident("backoff") => {
                    if seen.contains("backoff_max_ms") || !seen.insert("backoff".to_string()) {
                        return Err(err(path, "configure backoff only once"));
                    }
                    attrs.backoff_max_ms = Some(None);
                }
                Meta::NameValue(nv) => {
                    let ident = nv
                        .path
                        .get_ident()
                        .ok_or_else(|| err(&nv.path, "expected a simple attribute name"))?
                        .to_string();
                    if !seen.insert(ident.clone()) {
                        return Err(err(&nv.path, &format!("duplicate attribute `{ident}`")));
                    }
                    match ident.as_str() {
                        "name" => {
                            let name = string_value(&nv.value)?;
                            if name.is_empty() || name.len() > 255 || name.contains('\0') {
                                return Err(err(
                                    &nv.value,
                                    "job name must be 1..=255 bytes and contain no NUL",
                                ));
                            }
                            attrs.name = Some(name);
                        }
                        "max_attempts" => {
                            let max_attempts: u32 = int_value(&nv.value)?;
                            if max_attempts == 0 || max_attempts >= i32::MAX as u32 {
                                return Err(err(
                                    &nv.value,
                                    "max_attempts must be between 1 and 2147483646",
                                ));
                            }
                            attrs.max_attempts = Some(max_attempts);
                        }
                        "timeout_ms" => {
                            let timeout_ms = milliseconds_value(&nv.value)?;
                            attrs.timeout_ms = Some((timeout_ms != 0).then_some(timeout_ms));
                        }
                        "heartbeat_ms" => {
                            let heartbeat_ms = milliseconds_value(&nv.value)?;
                            if heartbeat_ms == 0 {
                                return Err(err(
                                    &nv.value,
                                    "heartbeat_ms must be greater than zero",
                                ));
                            }
                            attrs.heartbeat_ms = Some(heartbeat_ms);
                        }
                        "ttl_ms" => {
                            let ttl_ms = milliseconds_value(&nv.value)?;
                            attrs.ttl_ms = Some(if ttl_ms == 0 {
                                Ttl::Delete
                            } else {
                                Ttl::ForMs(ttl_ms)
                            });
                        }
                        "retry_delay_ms" => {
                            attrs.retry_delay_ms = Some(milliseconds_value(&nv.value)?)
                        }
                        "backoff_max_ms" => {
                            if seen.contains("backoff") {
                                return Err(err(&nv.path, "configure backoff only once"));
                            }
                            attrs.backoff_max_ms = Some(Some(milliseconds_value(&nv.value)?))
                        }
                        "priority" => attrs.priority = Some(priority_value(&nv.value)?),
                        "revision" => attrs.revision = Some(int_value(&nv.value)?),
                        other => {
                            return Err(err(
                                &nv.path,
                                &format!(
                                    "unknown attribute `{other}`; expected one of: name, \
                                     max_attempts, timeout_ms, heartbeat_ms, ttl_ms, retry_delay_ms, \
                                     backoff, backoff_max_ms, priority, revision"
                                ),
                            ));
                        }
                    }
                }
                other => {
                    return Err(err(
                        other,
                        "expected `key = value` (or bare `backoff`); see the pgqueue::job docs",
                    ));
                }
            }
        }
        Ok(attrs)
    }
}

fn err(spanned: &impl quote::ToTokens, message: &str) -> syn::Error {
    syn::Error::new_spanned(spanned, message)
}

/// Splits a required leading string literal (the cron expression) off an
/// attribute token stream, returning it and the remaining `key = value` args.
pub(crate) fn split_leading_str(tokens: TokenStream) -> syn::Result<(syn::LitStr, TokenStream)> {
    const EXPECTED: &str = "expected a cron expression string as the first argument, \
                            e.g. #[pgqueue::cron(\"0 * * * *\")]";

    struct Leading {
        lit: syn::LitStr,
        rest: TokenStream,
    }
    impl syn::parse::Parse for Leading {
        fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
            let lit: syn::LitStr = input
                .parse()
                .map_err(|e| syn::Error::new(e.span(), EXPECTED))?;
            let rest = if input.is_empty() {
                TokenStream::new()
            } else {
                input.parse::<Token![,]>()?;
                input.parse()?
            };
            Ok(Leading { lit, rest })
        }
    }

    if tokens.is_empty() {
        return Err(syn::Error::new(proc_macro2::Span::call_site(), EXPECTED));
    }
    let leading: Leading = syn::parse2(tokens)?;
    Ok((leading.lit, leading.rest))
}

fn string_value(expr: &Expr) -> syn::Result<String> {
    if let Expr::Lit(ExprLit {
        lit: Lit::Str(s), ..
    }) = expr
    {
        return Ok(s.value());
    }
    Err(err(expr, "expected a string literal"))
}

fn int_value<N: std::str::FromStr>(expr: &Expr) -> syn::Result<N> {
    if let Expr::Lit(ExprLit {
        lit: Lit::Int(int), ..
    }) = expr
        && int.suffix().is_empty()
        && let Ok(n) = int.base10_digits().parse()
    {
        return Ok(n);
    }
    Err(err(expr, "expected an unsuffixed integer literal"))
}

fn priority_value(expr: &Expr) -> syn::Result<i16> {
    // `-1` parses as a unary negation around a literal.
    if let Expr::Unary(ExprUnary {
        op: UnOp::Neg(_),
        expr: inner,
        ..
    }) = expr
    {
        let magnitude = int_value::<u16>(inner)?;
        if magnitude == i16::MAX as u16 + 1 {
            return Ok(i16::MIN);
        }
        let magnitude = i16::try_from(magnitude)
            .map_err(|_| err(expr, "priority must fit in a signed 16-bit integer"))?;
        return Ok(-magnitude);
    }
    int_value(expr)
}

fn milliseconds_value(expr: &Expr) -> syn::Result<u64> {
    // Keep in sync with pgqueue::job::MAX_DURATION (the macro crate cannot
    // depend on pgqueue without creating a dependency cycle).
    const MAX_DURATION_MS: u64 = 3_153_600_000_000;
    let value: u64 = int_value(expr)?;
    if value > MAX_DURATION_MS {
        return Err(err(expr, "milliseconds exceed pgqueue's supported range"));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    fn parse(tokens: TokenStream) -> syn::Result<JobAttrs> {
        JobAttrs::parse(tokens)
    }

    #[test]
    fn empty_attrs_are_all_none() {
        let attrs = parse(quote!()).unwrap();
        assert!(attrs.name.is_none());
        assert!(attrs.max_attempts.is_none());
        assert!(attrs.timeout_ms.is_none());
        assert!(attrs.heartbeat_ms.is_none());
        assert!(attrs.ttl_ms.is_none());
        assert!(attrs.retry_delay_ms.is_none());
        assert!(attrs.backoff_max_ms.is_none());
        assert!(attrs.priority.is_none());
        assert!(attrs.revision.is_none());
    }

    #[test]
    fn full_attribute_set_parses_every_supported_option() {
        let attrs = parse(quote!(
            name = "custom",
            max_attempts = 3,
            timeout_ms = 30_000,
            heartbeat_ms = 10_000,
            ttl_ms = 3_600_000,
            retry_delay_ms = 500,
            backoff_max_ms = 120_000,
            priority = -1,
            revision = 2
        ))
        .unwrap();
        assert_eq!(attrs.name.as_deref(), Some("custom"));
        assert_eq!(attrs.max_attempts, Some(3));
        assert_eq!(attrs.timeout_ms, Some(Some(30_000)));
        assert_eq!(attrs.heartbeat_ms, Some(10_000));
        assert!(matches!(attrs.ttl_ms, Some(Ttl::ForMs(3_600_000))));
        assert_eq!(attrs.retry_delay_ms, Some(500));
        assert_eq!(attrs.backoff_max_ms, Some(Some(120_000)));
        assert_eq!(attrs.priority, Some(-1));
        assert_eq!(attrs.revision, Some(2));
    }

    #[test]
    fn millisecond_values_are_not_scaled() {
        assert_eq!(
            parse(quote!(timeout_ms = 500)).unwrap().timeout_ms,
            Some(Some(500))
        );
        assert!(matches!(
            parse(quote!(ttl_ms = 500)).unwrap().ttl_ms,
            Some(Ttl::ForMs(500))
        ));
    }

    #[test]
    fn max_attempts_reserves_shutdown_refund_headroom() {
        assert!(parse(quote!(max_attempts = 2147483646)).is_ok());
        let error = parse(quote!(max_attempts = 2147483647)).unwrap_err();
        assert!(error.to_string().contains("2147483646"), "{error}");
    }

    #[test]
    fn zero_values_and_bare_backoff_parse_to_expected_values() {
        assert_eq!(
            parse(quote!(timeout_ms = 0)).unwrap().timeout_ms,
            Some(None)
        );
        assert!(matches!(
            parse(quote!(ttl_ms = 0)).unwrap().ttl_ms,
            Some(Ttl::Delete)
        ));
        assert_eq!(parse(quote!(backoff)).unwrap().backoff_max_ms, Some(None));
        assert_eq!(parse(quote!(priority = 7)).unwrap().priority, Some(7));
        assert_eq!(
            parse(quote!(priority = -32768)).unwrap().priority,
            Some(i16::MIN)
        );
    }

    #[test]
    fn attributes_reject_invalid_input() {
        for tokens in [
            quote!(bogus = 1),
            quote!(max_attempts = "three"),
            quote!(max_attempts = "3"),
            quote!(max_attempts = 0),
            quote!(max_attempts = 2147483648),
            quote!(timeout = 30_000),
            quote!(ttl = 60_000),
            quote!(timeout_ms = "30000"),
            quote!(timeout_ms = [1]),
            quote!(name = 42),
            quote!(priority = "high"),
            quote!(priority = -32769),
            quote!(heartbeat = 1_000),
            quote!(retry_delay = 500),
            quote!(backoff = 1),
            quote!(heartbeat_ms = 0),
            quote!(name = ""),
            quote!(max_attempts),
            quote!(timeout_ms = 1, timeout_ms = 2),
            quote!(backoff, backoff_max_ms = 1),
            quote!(timeout_ms = 99999999999999999999999),
            quote!(timeout_ms = 3153600000001),
        ] {
            assert!(parse(tokens.clone()).is_err(), "should reject: {tokens}");
        }
    }
}
