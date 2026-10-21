use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote, quote_spanned};
use syn::spanned::Spanned;
use syn::{
    Data, DeriveInput, Error, FnArg, GenericArgument, ImplItem, ImplItemFn, ItemImpl, Pat, PathArguments, ReturnType,
    Type, parse_macro_input,
};

struct StateOptions {
    key: Type,
    name: Option<String>,
    version: u32,
    migrate: Option<syn::Path>,
    locality: Option<syn::Path>,
}

fn parse_options(input: &DeriveInput) -> syn::Result<StateOptions> {
    let mut options = StateOptions {
        key: syn::parse_quote!(::std::string::String),
        name: None,
        version: 1,
        migrate: None,
        locality: None,
    };
    for attr in input.attrs.iter().filter(|a| a.path().is_ident("state")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("key") {
                options.key = meta.value()?.parse()?;
            } else if meta.path.is_ident("name") {
                let lit: syn::LitStr = meta.value()?.parse()?;
                options.name = Some(lit.value());
            } else if meta.path.is_ident("version") {
                let lit: syn::LitInt = meta.value()?.parse()?;
                options.version = lit.base10_parse()?;
            } else if meta.path.is_ident("migrate") {
                options.migrate = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("locality") {
                options.locality = Some(meta.value()?.parse()?);
            } else {
                return Err(meta.error("unknown #[state] option; expected key, name, version, migrate or locality"));
            }
            Ok(())
        })?;
    }
    Ok(options)
}

#[proc_macro_derive(State, attributes(state))]
pub fn derive_state(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    derive_state_impl(input).unwrap_or_else(Error::into_compile_error).into()
}

fn derive_state_impl(input: DeriveInput) -> syn::Result<TokenStream2> {
    if !input.generics.params.is_empty() {
        return Err(Error::new(
            input.generics.span(),
            "#[derive(quorium::State)] does not support generic types: every stored type needs one fixed NAME and encoding",
        ));
    }
    if matches!(input.data, Data::Union(_)) {
        return Err(Error::new(input.ident.span(), "#[derive(quorium::State)] is not supported on unions"));
    }
    let options = parse_options(&input)?;
    let ident = &input.ident;
    let vis = &input.vis;
    let handle = format_ident!("{}Handle", ident);
    let name = options.name.unwrap_or_else(|| ident.to_string());
    let version = options.version;
    let key = options.key;
    let migrate = options.migrate.map(|path| {
        quote! {
            fn migrate(from: u32, bytes: &[u8]) -> ::std::result::Result<Self, ::quorium::MigrationError> {
                #path(from, bytes)
            }
        }
    });
    let locality = options.locality.map(|path| {
        quote! {
            fn locality(key: &Self::Key) -> ::std::option::Option<::std::string::String> {
                ::std::option::Option::Some(::std::string::ToString::to_string(&#path(key)))
            }
        }
    });
    let escrow_fields: Vec<syn::Ident> = match &input.data {
        Data::Struct(data) => data
            .fields
            .iter()
            .filter(|f| match &f.ty {
                Type::Path(p) => p.path.segments.last().is_some_and(|s| s.ident == "Escrow"),
                _ => false,
            })
            .filter_map(|f| f.ident.clone())
            .collect(),
        _ => Vec::new(),
    };
    let escrow_names: Vec<String> = escrow_fields.iter().map(|f| f.to_string()).collect();
    let escrow_handles = escrow_fields.iter().map(|f| {
        let name = f.to_string();
        quote! {
            pub fn #f(&self) -> ::quorium::EscrowCounter {
                ::quorium::__private::escrow_counter(&self.entity, #name)
            }
        }
    });
    Ok(quote! {
        impl ::quorium::__private::EscrowFields for #ident {
            fn escrow_view(&self, field: &str) -> ::std::option::Option<&::quorium::__private::EscrowView> {
                match field {
                    #(#escrow_names => ::std::option::Option::Some(self.#escrow_fields.__view()),)*
                    _ => ::std::option::Option::None,
                }
            }

            fn escrow_view_mut(&mut self, field: &str) -> ::std::option::Option<&mut ::quorium::__private::EscrowView> {
                match field {
                    #(#escrow_names => ::std::option::Option::Some(self.#escrow_fields.__view_mut()),)*
                    _ => ::std::option::Option::None,
                }
            }
        }

        impl #handle {
            #(#escrow_handles)*
        }

        impl ::quorium::State for #ident {
            type Key = #key;
            type Handle = #handle;
            const NAME: &'static str = #name;
            const VERSION: u32 = #version;
            #migrate
            #locality
        }

        #[derive(Clone)]
        #vis struct #handle {
            entity: ::quorium::Entity<#ident>,
        }

        impl ::quorium::Handle for #handle {
            type State = #ident;
            fn from_entity(entity: ::quorium::Entity<#ident>) -> Self {
                Self { entity }
            }
        }

        impl #handle {
            pub async fn get(&self) -> ::std::result::Result<#ident, ::quorium::Error<::std::convert::Infallible>> {
                self.entity.get().await
            }

            pub async fn read<R>(&self, f: impl FnOnce(&#ident) -> R) -> ::std::result::Result<R, ::quorium::Error<::std::convert::Infallible>> {
                self.entity.read(f).await
            }

            pub async fn exists(&self) -> ::std::result::Result<bool, ::quorium::Error<::std::convert::Infallible>> {
                self.entity.exists().await
            }

            pub async fn delete(&self) -> ::std::result::Result<(), ::quorium::Error<::std::convert::Infallible>> {
                self.entity.delete().await
            }

            pub fn watch(&self) -> ::quorium::Watch<#ident> {
                self.entity.watch()
            }

            pub fn entity(&self) -> &::quorium::Entity<#ident> {
                &self.entity
            }

            pub fn stale(&self) -> Self {
                Self { entity: self.entity.stale() }
            }
        }
    })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Write,
    Read,
}

struct Method {
    kind: Kind,
    ident: syn::Ident,
    id: u32,
    uses_ctx: bool,
    args: Vec<(syn::Ident, Type)>,
    ret: Option<Type>,
    result: Option<(Type, Type)>,
    vis: syn::Visibility,
}

fn fnv1a32(s: &str) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for b in s.bytes() {
        hash ^= u32::from(b);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

fn result_parts(ty: &Type) -> Option<(Type, Type)> {
    let Type::Path(path) = ty else { return None };
    let last = path.path.segments.last()?;
    if last.ident != "Result" {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &last.arguments else { return None };
    let mut types = args.args.iter().filter_map(|a| match a {
        GenericArgument::Type(t) => Some(t.clone()),
        _ => None,
    });
    let ok = types.next()?;
    let err = types.next()?;
    Some((ok, err))
}

fn is_ctx(ty: &Type) -> bool {
    let Type::Reference(r) = ty else { return false };
    let Type::Path(p) = &*r.elem else { return false };
    r.mutability.is_none() && p.path.segments.last().is_some_and(|s| s.ident == "Ctx")
}

fn take_id(func: &mut ImplItemFn) -> syn::Result<Option<u32>> {
    let mut id = None;
    let mut error = None;
    func.attrs.retain(|attr| {
        if !attr.path().is_ident("quorium") {
            return true;
        }
        let parsed = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("id") {
                let lit: syn::LitInt = meta.value()?.parse()?;
                id = Some(lit.base10_parse()?);
                Ok(())
            } else {
                Err(meta.error("unknown #[quorium] option; expected id = N"))
            }
        });
        if let Err(e) = parsed {
            error = Some(e);
        }
        false
    });
    match error {
        Some(e) => Err(e),
        None => Ok(id),
    }
}

fn analyse(func: &mut ImplItemFn) -> syn::Result<Option<Method>> {
    let pinned = take_id(func)?;
    let Some(FnArg::Receiver(receiver)) = func.sig.inputs.first() else { return Ok(None) };
    if receiver.reference.is_none() {
        return Ok(None);
    }
    let kind = if receiver.mutability.is_some() { Kind::Write } else { Kind::Read };
    if kind == Kind::Write && func.sig.asyncness.is_some() {
        return Err(Error::new(
            func.sig.asyncness.span(),
            "write methods (&mut self) must be synchronous: they run inside the replicated apply loop, where every replica must reach the same result without waiting on anything",
        ));
    }
    if kind == Kind::Write && !func.sig.generics.params.is_empty() {
        return Err(Error::new(
            func.sig.generics.span(),
            "write methods (&mut self) cannot be generic: each write is dispatched by a fixed method ID with one argument encoding",
        ));
    }
    if kind == Kind::Read && (func.sig.asyncness.is_some() || !func.sig.generics.params.is_empty()) {
        return Ok(None);
    }
    let mut args = Vec::new();
    let mut uses_ctx = false;
    for (i, input) in func.sig.inputs.iter().skip(1).enumerate() {
        let FnArg::Typed(pt) = input else { continue };
        if i == 0 && is_ctx(&pt.ty) {
            if kind == Kind::Read {
                return Err(Error::new(
                    pt.ty.span(),
                    "ctx: &Ctx is only available to write methods (&mut self); reads run locally and have no log index or replicated clock",
                ));
            }
            uses_ctx = true;
            continue;
        }
        let Pat::Ident(name) = &*pt.pat else {
            return Err(Error::new(pt.pat.span(), "quorium methods need simple argument names"));
        };
        args.push((name.ident.clone(), (*pt.ty).clone()));
    }
    let ret = match &func.sig.output {
        ReturnType::Default => None,
        ReturnType::Type(_, ty) => Some((**ty).clone()),
    };
    let result = ret.as_ref().and_then(result_parts);
    let ident = func.sig.ident.clone();
    let id = pinned.unwrap_or_else(|| fnv1a32(&ident.to_string()));
    Ok(Some(Method { kind, ident, id, uses_ctx, args, ret, result, vis: func.vis.clone() }))
}

#[proc_macro_attribute]
pub fn state(attr: TokenStream, item: TokenStream) -> TokenStream {
    if !attr.is_empty() {
        return Error::new(Span::call_site(), "#[quorium::state] takes no arguments").into_compile_error().into();
    }
    let item = parse_macro_input!(item as ItemImpl);
    state_impl(item).unwrap_or_else(Error::into_compile_error).into()
}

fn state_impl(mut item: ItemImpl) -> syn::Result<TokenStream2> {
    if item.trait_.is_some() {
        return Err(Error::new(item.span(), "#[quorium::state] goes on an inherent impl block, not a trait impl"));
    }
    if !item.generics.params.is_empty() {
        return Err(Error::new(item.generics.span(), "#[quorium::state] does not support generic impl blocks"));
    }
    let Type::Path(self_path) = &*item.self_ty else {
        return Err(Error::new(item.self_ty.span(), "#[quorium::state] expects `impl TypeName`"));
    };
    let state_ident = self_path.path.segments.last().expect("type path has a segment").ident.clone();
    let state_ty = item.self_ty.clone();
    let handle = format_ident!("{}Handle", state_ident);

    let mut methods = Vec::new();
    let mut errors: Option<Error> = None;
    for impl_item in item.items.iter_mut() {
        if let ImplItem::Fn(func) = impl_item {
            match analyse(func) {
                Ok(Some(m)) => methods.push(m),
                Ok(None) => {}
                Err(e) => match &mut errors {
                    Some(all) => all.combine(e),
                    None => errors = Some(e),
                },
            }
        }
    }
    let writes: Vec<&Method> = methods.iter().filter(|m| m.kind == Kind::Write).collect();
    for m in &writes {
        if m.id == 0xFFFF_FF00 {
            let e = Error::new(
                m.ident.span(),
                "write ID 0xFFFFFF00 is reserved for escrow fields; pin another #[quorium(id = N)]",
            );
            match &mut errors {
                Some(all) => all.combine(e),
                None => errors = Some(e),
            }
        }
    }
    for (i, a) in writes.iter().enumerate() {
        if let Some(b) = writes[..i].iter().find(|b| b.id == a.id) {
            let e = Error::new(
                a.ident.span(),
                format!(
                    "write method `{}` has ID {} which is already used by `{}`; pin a different #[quorium(id = N)]",
                    a.ident, a.id, b.ident
                ),
            );
            match &mut errors {
                Some(all) => all.combine(e),
                None => errors = Some(e),
            }
        }
    }
    if let Some(e) = errors {
        return Err(e);
    }

    let mut handle_methods = Vec::new();
    let mut checks = Vec::new();
    let mut arms = Vec::new();
    for m in &methods {
        let ident = &m.ident;
        let vis = &m.vis;
        let id = m.id;
        let names: Vec<&syn::Ident> = m.args.iter().map(|(n, _)| n).collect();
        let types: Vec<&Type> = m.args.iter().map(|(_, t)| t).collect();
        for (_, ty) in &m.args {
            checks.push(quote_spanned! {ty.span()=> ::quorium::__private::assert_argument::<#ty>(); });
        }
        if let Some(ret) = &m.ret {
            checks.push(quote_spanned! {ret.span()=> ::quorium::__private::assert_return::<#ret>(); });
        }
        match m.kind {
            Kind::Write => {
                let call_args = if m.uses_ctx {
                    quote! { ctx, #(#names),* }
                } else {
                    quote! { #(#names),* }
                };
                arms.push(quote! {
                    #id => {
                        let (#(#names,)*): (#(#types,)*) = ::quorium::__private::decode_args(args)?;
                        let reply = state.#ident(#call_args);
                        Ok((::quorium::__private::encode(&state)?, ::quorium::__private::encode(&reply)?))
                    }
                });
                let (output, body) = match (&m.result, &m.ret) {
                    (Some((ok, err)), _) => (
                        quote! { ::std::result::Result<#ok, ::quorium::Error<#err>> },
                        quote! { self.entity.write_result::<#ok, #err>(#id, &(#(#names,)*)).await },
                    ),
                    (None, Some(ret)) => (
                        quote! { ::std::result::Result<#ret, ::quorium::Error<::std::convert::Infallible>> },
                        quote! { self.entity.write::<#ret>(#id, &(#(#names,)*)).await },
                    ),
                    (None, None) => (
                        quote! { ::std::result::Result<(), ::quorium::Error<::std::convert::Infallible>> },
                        quote! { self.entity.write::<()>(#id, &(#(#names,)*)).await },
                    ),
                };
                handle_methods.push(quote! {
                    #vis async fn #ident(&self, #(#names: #types),*) -> #output {
                        #body
                    }
                });
            }
            Kind::Read => {
                let (output, body) = match (&m.result, &m.ret) {
                    (Some((ok, err)), _) => (
                        quote! { ::std::result::Result<#ok, ::quorium::Error<#err>> },
                        quote! { self.entity.read_result(move |s: &#state_ty| s.#ident(#(#names),*)).await },
                    ),
                    (None, Some(ret)) => (
                        quote! { ::std::result::Result<#ret, ::quorium::Error<::std::convert::Infallible>> },
                        quote! { self.entity.read(move |s: &#state_ty| s.#ident(#(#names),*)).await },
                    ),
                    (None, None) => (
                        quote! { ::std::result::Result<(), ::quorium::Error<::std::convert::Infallible>> },
                        quote! { self.entity.read(move |s: &#state_ty| s.#ident(#(#names),*)).await },
                    ),
                };
                handle_methods.push(quote! {
                    #vis async fn #ident(&self, #(#names: #types),*) -> #output {
                        #body
                    }
                });
            }
        }
    }
    let ids: Vec<u32> = writes.iter().map(|m| m.id).collect();
    let register = format_ident!("__QUORIUM_REGISTRATION_{}", state_ident.to_string().to_uppercase());
    Ok(quote! {
        #item

        impl #handle {
            #(#handle_methods)*
        }

        const _: () = {
            #[allow(dead_code)]
            fn __quorium_checks() {
                #(#checks)*
            }

            fn __quorium_apply(
                stored: ::std::option::Option<&[u8]>,
                method: u32,
                args: &[u8],
                ctx: &::quorium::Ctx,
            ) -> ::std::result::Result<(::std::vec::Vec<u8>, ::std::vec::Vec<u8>), ::quorium::__private::ApplyError> {
                let _ = ctx;
                let mut state: #state_ty = ::quorium::__private::decode_state(stored)?;
                match method {
                    #(#arms)*
                    ::quorium::__private::ESCROW_METHOD => {
                        let reply = ::quorium::__private::escrow_field_apply(&mut state, args)?;
                        Ok((::quorium::__private::encode(&state)?, reply))
                    }
                    other => Err(::quorium::__private::ApplyError::UnknownMethod(other)),
                }
            }

            #[::quorium::__private::linkme::distributed_slice(::quorium::__private::REGISTRY)]
            #[linkme(crate = ::quorium::__private::linkme)]
            static #register: ::quorium::__private::Registration = ::quorium::__private::Registration {
                name: <#state_ty as ::quorium::State>::NAME,
                version: <#state_ty as ::quorium::State>::VERSION,
                methods: &[#(#ids,)* ::quorium::__private::ESCROW_METHOD],
                apply: __quorium_apply,
                migrate: ::std::option::Option::Some(::quorium::__private::migrate::<#state_ty>),
            };

            impl ::quorium::__private::Registered for #state_ty {
                fn registration() -> &'static ::quorium::__private::Registration {
                    &#register
                }
            }
        };
    })
}
