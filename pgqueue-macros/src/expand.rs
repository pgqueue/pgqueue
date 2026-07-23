//! The `#[pgqueue::job]` / `#[pgqueue::cron]` expansions, kept as pure token
//! transforms so they can be unit-tested without compiling user code.

use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use syn::{FnArg, Ident, ItemFn, ReturnType, Type};

use crate::attrs::{JobAttrs, Ttl, split_leading_str};

/// Which attribute is expanding; drives the signature contract and the
/// generated `SCHEDULE`.
enum Mode {
    /// `#[pgqueue::job]`: first param is the payload, the rest are extractors.
    Job,
    /// `#[pgqueue::cron("...")]`: every param is an extractor; the payload is
    /// fixed to `()` and the schedule is baked into the job type.
    Cron { schedule: String },
}

impl Mode {
    fn attr_name(&self) -> &'static str {
        match self {
            Mode::Job => "#[pgqueue::job]",
            Mode::Cron { .. } => "#[pgqueue::cron]",
        }
    }
}

/// Expands `#[pgqueue::job(...)]`.
pub(crate) fn expand_job(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let attrs = JobAttrs::parse(attr)?;
    if attrs.revision.is_some() {
        return Err(syn::Error::new(
            Span::call_site(),
            "`revision` is only valid on #[pgqueue::cron]",
        ));
    }
    expand(Mode::Job, attrs, item)
}

/// Expands `#[pgqueue::cron("expr", ...)]`, validating the cron expression at
/// compile time (same parser the worker uses at runtime).
pub(crate) fn expand_cron(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let (expr, rest) = split_leading_str(attr)?;
    let schedule = expr.value();
    cron_schedule::parser::CronParser::builder()
        .seconds(cron_schedule::parser::Seconds::Optional)
        .build()
        .parse(&schedule)
        .map_err(|e| {
            syn::Error::new_spanned(&expr, format!("invalid cron expression {schedule:?}: {e}"))
        })?;
    let attrs = JobAttrs::parse(rest)?;
    expand(Mode::Cron { schedule }, attrs, item)
}

/// Expands the annotated function into:
/// 1. a unit struct named after the function (the job's handle),
/// 2. `::job(args)` (or zero-arg `::job()` for cron) — the typed enqueue
///    constructor,
/// 3. `::call(...)` — a direct invoker preserving the original signature,
/// 4. a `JobType` impl carrying name/config/schedule and the erased handler.
fn expand(mode: Mode, attrs: JobAttrs, item: TokenStream) -> syn::Result<TokenStream> {
    let func: ItemFn = syn::parse2(item)?;
    validate(&mode, &func)?;
    let runtime = runtime_crate_path();

    let vis = &func.vis;
    let ident = &func.sig.ident;
    let name = attrs.name.clone().unwrap_or_else(|| ident.to_string());
    let docs: Vec<_> = func
        .attrs
        .iter()
        .filter(|a| a.path().is_ident("doc"))
        .collect();

    let mut types = Vec::new();
    for input in &func.sig.inputs {
        match input {
            FnArg::Typed(pat) => types.push((*pat.ty).clone()),
            FnArg::Receiver(receiver) => {
                return Err(syn::Error::new_spanned(
                    receiver,
                    format!("{} functions cannot take self", mode.attr_name()),
                ));
            }
        }
    }
    // Job mode: first param is the payload, the rest are extractors.
    // Cron mode: every param is an extractor; the payload is `()`.
    let (payload_ty, extractor_tys): (Type, &[Type]) = match mode {
        Mode::Job => (types[0].clone(), &types[1..]),
        Mode::Cron { .. } => (syn::parse_quote!(()), &types[..]),
    };

    let ret_ty: Type = match &func.sig.output {
        ReturnType::Default => syn::parse_quote!(()),
        ReturnType::Type(_, ty) => (**ty).clone(),
    };

    // `call()` forwards positionally with fresh names (original patterns may
    // be `_` or destructurings).
    let call_names: Vec<_> = (0..types.len())
        .map(|i| format_ident!("__arg{i}", span = Span::call_site()))
        .collect();
    let call_params: Vec<_> = call_names
        .iter()
        .zip(&types)
        .map(|(name, ty)| quote!(#name: #ty))
        .collect();

    // The original function, moved verbatim (body, params, output) under a
    // private name inside the anonymous const.
    let mut inner = func.clone();
    inner.vis = syn::Visibility::Inherited;
    inner.sig.ident = format_ident!("__pgqueue_inner");
    inner.attrs.retain(|a| !a.path().is_ident("doc"));

    let config_setters = config_setters(&attrs, &runtime);

    let extractor_names: Vec<_> = (0..extractor_tys.len())
        .map(|i| format_ident!("__ext{i}", span = Span::call_site()))
        .collect();
    let extractions: Vec<_> = extractor_names
        .iter()
        .zip(extractor_tys)
        .map(|(name, ty)| {
            quote! {
                let #name = <#ty as #runtime::FromJobContext>::from_context(&__ctx)?;
            }
        })
        .collect();

    // Cron handlers take no payload, so the erased call skips `__args`.
    let (job_ctor, invoke, schedule_const) = match &mode {
        Mode::Job => (
            quote! {
                /// Builds a typed enqueue request for this job
                /// (pass it to `Queue::enqueue`).
                #vis fn job(args: #payload_ty) -> #runtime::JobBuilder<#ident> {
                    #runtime::JobBuilder::new(args)
                }
            },
            quote!(__pgqueue_inner(__args #(, #extractor_names)*)),
            quote!(),
        ),
        Mode::Cron { schedule } => {
            let revision = attrs.revision.unwrap_or(0);
            (
                quote! {
                    /// Builds an enqueue request for a one-off, out-of-schedule
                    /// run of this cron job (pass it to `Queue::enqueue`).
                    #vis fn job() -> #runtime::JobBuilder<#ident> {
                        #runtime::JobBuilder::new(())
                    }
                },
                quote!(__pgqueue_inner(#(#extractor_names),*)),
                quote! {
                    const SCHEDULE: ::core::option::Option<&'static str> =
                        ::core::option::Option::Some(#schedule);
                    const CRON_REVISION: u64 = #revision;
                },
            )
        }
    };
    let decode_args = match &mode {
        Mode::Job => quote! {
            let __args: #payload_ty = #runtime::__private::decode_payload(__payload)?;
        },
        // The payload is always `()`/null for cron jobs; nothing to decode.
        Mode::Cron { .. } => quote!(let _ = __payload;),
    };

    Ok(quote! {
        #(#docs)*
        #[allow(non_camel_case_types)]
        #[derive(::core::clone::Clone, ::core::marker::Copy, ::core::fmt::Debug)]
        #vis struct #ident;

        const _: () = {
            #inner

            impl #ident {
                #job_ctor

                /// Invokes the underlying handler function directly,
                /// bypassing the queue — useful in unit tests.
                #vis async fn call(#(#call_params),*) -> #ret_ty {
                    __pgqueue_inner(#(#call_names),*).await
                }
            }

            impl #runtime::JobType for #ident {
                type Args = #payload_ty;
                type Output = <#ret_ty as #runtime::__private::IntoJobResult>::Output;
                const NAME: &'static str = #name;
                #schedule_const

                fn config() -> #runtime::JobConfig {
                    #[allow(unused_mut)]
                    let mut config = #runtime::JobConfig::default();
                    #(#config_setters)*
                    config
                }

                fn erased() -> #runtime::__private::TypeErasedJobHandler {
                    #runtime::__private::TypeErasedJobHandler::new::<Self>(|__payload, __ctx| {
                        ::std::boxed::Box::pin(async move {
                            #decode_args
                            #(#extractions)*
                            #runtime::__private::encode_result(#invoke.await)
                        })
                    })
                }
            }
        };
    })
}

fn runtime_crate_path() -> TokenStream {
    match crate_name("pgqueue") {
        Ok(found) => found_crate_path(found),
        Err(_) => quote!(::pgqueue),
    }
}

fn found_crate_path(found: FoundCrate) -> TokenStream {
    match found {
        FoundCrate::Name(name) => {
            let ident = Ident::new(&name, Span::call_site());
            quote!(::#ident)
        }
        // `pgqueue` exposes this self-alias so the same absolute path works in
        // library code, doctests, and package integration tests.
        FoundCrate::Itself => quote!(::pgqueue),
    }
}

fn validate(mode: &Mode, func: &ItemFn) -> syn::Result<()> {
    if func.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            func.sig.fn_token,
            format!("{} functions must be async", mode.attr_name()),
        ));
    }
    if let Some(unsafety) = &func.sig.unsafety {
        return Err(syn::Error::new_spanned(
            unsafety,
            format!("{} functions cannot be unsafe", mode.attr_name()),
        ));
    }
    if !func.sig.generics.params.is_empty() || func.sig.generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(
            &func.sig.generics,
            format!("{} functions cannot be generic", mode.attr_name()),
        ));
    }
    if matches!(mode, Mode::Job) && func.sig.inputs.is_empty() {
        return Err(syn::Error::new_spanned(
            &func.sig.ident,
            "#[pgqueue::job] functions need a payload as their first parameter; \
             use `_: ()` for jobs without one",
        ));
    }
    if let Some(variadic) = &func.sig.variadic {
        return Err(syn::Error::new_spanned(
            variadic,
            format!("{} functions cannot be variadic", mode.attr_name()),
        ));
    }
    Ok(())
}

fn config_setters(attrs: &JobAttrs, runtime: &TokenStream) -> Vec<TokenStream> {
    let mut setters = Vec::new();
    if let Some(max_attempts) = attrs.max_attempts {
        setters.push(quote!(config.max_attempts = #max_attempts;));
    }
    if let Some(timeout) = &attrs.timeout_ms {
        setters.push(match timeout {
            Some(ms) => quote! {
                config.timeout =
                    ::core::option::Option::Some(::core::time::Duration::from_millis(#ms));
            },
            None => quote!(config.timeout = ::core::option::Option::None;),
        });
    }
    if let Some(ms) = attrs.heartbeat_ms {
        setters.push(quote! {
            config.heartbeat =
                ::core::option::Option::Some(::core::time::Duration::from_millis(#ms));
        });
    }
    if let Some(ttl) = &attrs.ttl_ms {
        setters.push(match ttl {
            Ttl::ForMs(ms) => quote! {
                config.retention =
                    #runtime::JobRetention::For(::core::time::Duration::from_millis(#ms));
            },
            Ttl::Delete => quote!(config.retention = #runtime::JobRetention::DeleteImmediately;),
        });
    }
    if let Some(ms) = attrs.retry_delay_ms {
        setters.push(quote!(config.retry_delay = ::core::time::Duration::from_millis(#ms);));
    }
    if let Some(backoff) = &attrs.backoff_max_ms {
        setters.push(match backoff {
            Some(ms) => quote! {
                config.backoff = #runtime::JobRetryBackoff::Exponential {
                    max: ::core::option::Option::Some(::core::time::Duration::from_millis(#ms)),
                };
            },
            None => quote! {
                config.backoff =
                    #runtime::JobRetryBackoff::Exponential { max: ::core::option::Option::None };
            },
        });
    }
    if let Some(priority) = attrs.priority {
        setters.push(quote!(config.priority = #priority;));
    }
    setters
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    fn expand_ok(attr: TokenStream, item: TokenStream) -> String {
        expand_job(attr, item)
            .map(|t| t.to_string())
            .unwrap_or_else(|e| panic!("{e}"))
    }

    fn expand_cron_ok(attr: TokenStream, item: TokenStream) -> String {
        expand_cron(attr, item)
            .map(|t| t.to_string())
            .unwrap_or_else(|e| panic!("{e}"))
    }

    fn compact(s: &str) -> String {
        s.replace(' ', "")
    }

    #[test]
    fn runtime_crate_path_uses_dependency_alias() {
        let path = found_crate_path(FoundCrate::Name("myqueue".to_string()));
        assert_eq!(compact(&path.to_string()), "::myqueue");
    }

    #[test]
    fn expands_minimal_job() {
        let out = expand_ok(
            quote!(),
            quote! {
                async fn send_email(args: SendEmail) -> anyhow::Result<()> {
                    Ok(())
                }
            },
        );
        let flat = compact(&out);
        assert!(flat.contains("structsend_email;"), "{out}");
        assert!(
            flat.contains("impl::pgqueue::JobTypeforsend_email"),
            "{out}"
        );
        assert!(flat.contains("typeArgs=SendEmail;"), "{out}");
        assert!(
            flat.contains(
                "typeOutput=<anyhow::Result<()>as::pgqueue::__private::IntoJobResult>::Output;"
            ),
            "{out}"
        );
        assert!(
            flat.contains("constNAME:&'staticstr=\"send_email\";"),
            "{out}"
        );
        assert!(flat.contains("fnjob(args:SendEmail)"), "{out}");
        assert!(
            flat.contains("asyncfncall(__arg0:SendEmail)->anyhow::Result<()>"),
            "{out}"
        );
        assert!(flat.contains("asyncfn__pgqueue_inner"), "{out}");
        // No attrs: config is just the default; no schedule for plain jobs.
        assert!(!flat.contains("config.max_attempts="), "{out}");
        assert!(!flat.contains("SCHEDULE"), "{out}");
    }

    #[test]
    fn expands_extractors_positionally() {
        let out = expand_ok(
            quote!(),
            quote! {
                pub async fn resize(args: Resize, s: JobState<Pool>, ctx: JobContext) -> Result<u32, Error> {
                    Ok(1)
                }
            },
        );
        let flat = compact(&out);
        assert!(flat.contains("pubstructresize;"), "{out}");
        assert!(
            flat.contains("<JobState<Pool>as::pgqueue::FromJobContext>::from_context(&__ctx)"),
            "{out}"
        );
        assert!(
            flat.contains("<JobContextas::pgqueue::FromJobContext>::from_context(&__ctx)"),
            "{out}"
        );
        assert!(
            flat.contains("__pgqueue_inner(__args,__ext0,__ext1)"),
            "{out}"
        );
        assert!(
            flat.contains("pubasyncfncall(__arg0:Resize,__arg1:JobState<Pool>,__arg2:JobContext)"),
            "{out}"
        );
    }

    #[test]
    fn expands_all_config_attrs() {
        let out = expand_ok(
            quote!(
                name = "custom",
                max_attempts = 4,
                timeout_ms = 30_000,
                heartbeat_ms = 10_000,
                ttl_ms = 3_600_000,
                retry_delay_ms = 500,
                backoff_max_ms = 120_000,
                priority = -1
            ),
            quote! {
                async fn j(_: ()) {}
            },
        );
        let flat = compact(&out);
        assert!(flat.contains("constNAME:&'staticstr=\"custom\";"), "{out}");
        assert!(flat.contains("config.max_attempts=4u32;"), "{out}");
        assert!(flat.contains("config.timeout=::core::option::Option::Some(::core::time::Duration::from_millis(30000u64));"), "{out}");
        assert!(flat.contains("config.heartbeat="), "{out}");
        assert!(
            flat.contains(
                "::pgqueue::JobRetention::For(::core::time::Duration::from_millis(3600000u64))"
            ),
            "{out}"
        );
        assert!(
            flat.contains("config.retry_delay=::core::time::Duration::from_millis(500u64);"),
            "{out}"
        );
        assert!(
            flat.contains(
                "::pgqueue::JobRetryBackoff::Exponential{max:::core::option::Option::Some"
            ),
            "{out}"
        );
        assert!(flat.contains("config.priority=-1i16;"), "{out}");
        // Unit return type maps through IntoJobResult for ().
        assert!(
            flat.contains("typeOutput=<()as::pgqueue::__private::IntoJobResult>::Output;"),
            "{out}"
        );
    }

    #[test]
    fn expands_zero_values_and_bare_backoff() {
        let out = expand_ok(
            quote!(timeout_ms = 0, backoff),
            quote! {
                async fn j(_: ()) {}
            },
        );
        let flat = compact(&out);
        assert!(
            flat.contains("config.timeout=::core::option::Option::None;"),
            "{out}"
        );
        assert!(!flat.contains("config.retention="), "{out}");
        assert!(
            flat.contains("JobRetryBackoff::Exponential{max:::core::option::Option::None}"),
            "{out}"
        );

        let out = expand_ok(
            quote!(ttl_ms = 0),
            quote!(
                async fn j(_: ()) {}
            ),
        );
        assert!(
            compact(&out).contains("JobRetention::DeleteImmediately"),
            "{out}"
        );
    }

    #[test]
    fn keeps_doc_comments_on_the_struct() {
        let out = expand_ok(
            quote!(),
            quote! {
                /// Sends the welcome email.
                async fn welcome(_: ()) {}
            },
        );
        assert!(out.contains("Sends the welcome email."), "{out}");
    }

    #[test]
    fn rejects_invalid_functions() {
        let cases: Vec<(TokenStream, &str)> = vec![
            (
                quote!(
                    fn j(_: ()) {}
                ),
                "must be async",
            ),
            (
                quote!(
                    async fn j() {}
                ),
                "need a payload",
            ),
            (
                quote!(
                    async fn j<T>(args: T) {}
                ),
                "cannot be generic",
            ),
            (
                quote!(
                    async unsafe fn j(_: ()) {}
                ),
                "cannot be unsafe",
            ),
            (
                quote! {
                    async fn j(args: u32) where u32: Copy {}
                },
                "cannot be generic",
            ),
            (
                quote!(
                    async fn j(self, args: u32) {}
                ),
                "cannot take self",
            ),
        ];
        for (item, expected) in cases {
            let err =
                expand_job(quote!(), item.clone()).expect_err(&format!("should fail: {item}"));
            assert!(err.to_string().contains(expected), "{item}: {err}");
        }
    }

    #[test]
    fn attr_errors_propagate() {
        let err = expand_job(
            quote!(bogus = 1),
            quote!(
                async fn j(_: ()) {}
            ),
        )
        .unwrap_err();
        assert!(err.to_string().contains("unknown attribute"), "{err}");
        let err = expand_job(
            quote!(),
            quote!(
                struct NotAFn;
            ),
        )
        .unwrap_err();
        assert!(err.to_string().contains("expected"), "{err}");
    }

    #[test]
    fn expands_cron_with_extractors_only() {
        let out = expand_cron_ok(
            quote!("*/5 * * * *"),
            quote! {
                pub async fn cleanup(ctx: JobContext, db: JobState<Pool>) -> anyhow::Result<u64> {
                    Ok(0)
                }
            },
        );
        let flat = compact(&out);
        assert!(flat.contains("pubstructcleanup;"), "{out}");
        // Payload is fixed to () and job() takes no arguments.
        assert!(flat.contains("typeArgs=();"), "{out}");
        assert!(
            flat.contains("pubfnjob()->::pgqueue::JobBuilder<cleanup>"),
            "{out}"
        );
        assert!(flat.contains("::pgqueue::JobBuilder::new(())"), "{out}");
        // The schedule is baked in.
        assert!(
            flat.contains(
                "constSCHEDULE:::core::option::Option<&'staticstr>=::core::option::Option::Some(\"*/5****\");"
            ),
            "{out}"
        );
        // Every parameter is an extractor; no payload decode.
        assert!(
            flat.contains("<JobContextas::pgqueue::FromJobContext>::from_context(&__ctx)"),
            "{out}"
        );
        assert!(
            flat.contains("<JobState<Pool>as::pgqueue::FromJobContext>::from_context(&__ctx)"),
            "{out}"
        );
        assert!(flat.contains("__pgqueue_inner(__ext0,__ext1)"), "{out}");
        assert!(!flat.contains("decode_payload"), "{out}");
        // call() preserves the original extractor-only signature.
        assert!(
            flat.contains("pubasyncfncall(__arg0:JobContext,__arg1:JobState<Pool>)"),
            "{out}"
        );
    }

    #[test]
    fn expands_cron_with_no_params_and_config() {
        let out = expand_cron_ok(
            quote!(
                "30 */5 * * * *",
                name = "tidy",
                max_attempts = 2,
                timeout_ms = 300_000
            ),
            quote! {
                async fn cleanup() {}
            },
        );
        let flat = compact(&out);
        assert!(flat.contains("constNAME:&'staticstr=\"tidy\";"), "{out}");
        assert!(flat.contains("config.max_attempts=2u32;"), "{out}");
        assert!(flat.contains("Some(\"30*/5****\")"), "{out}");
        assert!(flat.contains("__pgqueue_inner()"), "{out}");
    }

    #[test]
    fn cron_rejects_bad_input() {
        // Missing expression.
        let err = expand_cron(
            quote!(),
            quote!(
                async fn j() {}
            ),
        )
        .unwrap_err();
        assert!(err.to_string().contains("cron expression"), "{err}");
        // Non-string expression.
        let err = expand_cron(
            quote!(42),
            quote!(
                async fn j() {}
            ),
        )
        .unwrap_err();
        assert!(err.to_string().contains("cron expression"), "{err}");
        // Invalid expression (validated at compile time).
        let err = expand_cron(
            quote!("99 * * * *"),
            quote!(
                async fn j() {}
            ),
        )
        .unwrap_err();
        assert!(err.to_string().contains("invalid cron expression"), "{err}");
        // Bad config attr after the expression.
        let err = expand_cron(
            quote!("* * * * *", bogus = 1),
            quote!(
                async fn j() {}
            ),
        )
        .unwrap_err();
        assert!(err.to_string().contains("unknown attribute"), "{err}");
        // Signature rules still apply.
        let err = expand_cron(
            quote!("* * * * *"),
            quote!(
                fn j() {}
            ),
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("#[pgqueue::cron] functions must be async"),
            "{err}"
        );

        let err = expand_cron(
            quote!("* * * * *"),
            quote!(
                async unsafe fn j() {}
            ),
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("#[pgqueue::cron] functions cannot be unsafe"),
            "{err}"
        );
    }
}
