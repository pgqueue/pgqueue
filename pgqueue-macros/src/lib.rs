//! Procedural macros for the `pgqueue` crate.
//!
//! Do not depend on this crate directly; use the re-export at `pgqueue::job`.

use proc_macro::TokenStream;

mod attrs;
mod expand;

/// Marks an `async fn` as a pgqueue job handler.
///
/// See the `pgqueue` crate documentation for the accepted attributes and the
/// function signature contract.
#[proc_macro_attribute]
pub fn job(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand::expand_job(attr.into(), item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Marks an `async fn` as a pgqueue cron job, run on the given schedule.
///
/// The first argument is the cron expression (validated at compile time);
/// the rest are the same configuration attributes as `job`. Cron functions
/// take no payload — every parameter is an extractor.
///
/// See the `pgqueue` crate documentation for details.
#[proc_macro_attribute]
pub fn cron(attr: TokenStream, item: TokenStream) -> TokenStream {
    expand::expand_cron(attr.into(), item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
