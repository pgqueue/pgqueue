//! Procedural macros for the `pgqueue` crate.
//!
//! Do not depend on this crate directly; use the re-export at `pgqueue::job`.

use proc_macro::TokenStream;

mod attrs;
mod expand;

/// Marks an `async fn` as a pgqueue job handler.
///
/// The accepted attributes and the signature contract are documented on the
/// re-export this is used through, `pgqueue::job`.
#[proc_macro_attribute]
pub fn job(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand::expand_job(attr.into(), item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Marks an `async fn` as a pgqueue cron job, run on the given schedule.
///
/// The first argument is the cron expression (syntax-checked at compile time);
/// the rest are the same configuration attributes as `job`. A syntactically
/// valid expression with no future UTC occurrence disables that cron on the
/// worker and degrades its scheduler health rather than stopping it. Cron
/// functions take no payload — every parameter is an extractor.
///
/// The accepted attributes are documented on the re-export this is used
/// through, `pgqueue::cron`.
#[proc_macro_attribute]
pub fn cron(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand::expand_cron(attr.into(), item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
