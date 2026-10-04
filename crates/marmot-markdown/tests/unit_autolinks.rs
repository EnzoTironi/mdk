use marmot_markdown::{AutolinkKind, Inline, LinkDestinationKind, NostrHrp};

mod common;
use common::{parse_inlines, t};

fn uri(url: &str) -> Inline {
    Inline::Autolink {
        url: url.to_string(),
        kind: AutolinkKind::Uri,
        classification: marmot_markdown::classify_link_destination(url),
    }
}
fn email(addr: &str) -> Inline {
    Inline::Autolink {
        url: addr.to_string(),
        kind: AutolinkKind::Email,
        classification: LinkDestinationKind::Contact,
    }
}
fn www(url: &str) -> Inline {
    Inline::Autolink {
        url: url.to_string(),
        kind: AutolinkKind::Www,
        classification: LinkDestinationKind::Web,
    }
}

// HTML is NOT parsed: tag-like sequences are literal text in both the
// block pass (no HTML blocks) and the inline pass (no raw-HTML inline).
// Only autolinks — `<scheme:body>` and `<email@host>` — get structured
// treatment.

// ----- URI autolinks ----------------------------------------------------

#[test]
fn autolink_https() {
    assert_eq!(
        parse_inlines("x <https://example.com/path?q=1>"),
        vec![t("x "), uri("https://example.com/path?q=1")]
    );
}

#[test]
fn autolink_http() {
    assert_eq!(
        parse_inlines("x <http://x>"),
        vec![t("x "), uri("http://x")]
    );
}

#[test]
fn autolink_with_surrounding_text() {
    assert_eq!(
        parse_inlines("see <http://x> please"),
        vec![t("see "), uri("http://x"), t(" please")]
    );
}

#[test]
fn autolink_scheme_too_short_rejected() {
    // Single-letter scheme is invalid; falls through to literal text.
    assert_eq!(parse_inlines("x <a:b>"), vec![t("x <a:b>")]);
}

#[test]
fn autolink_with_space_rejected() {
    assert_eq!(parse_inlines("x <http://x y>"), vec![t("x <http://x y>")]);
}

// ----- Email autolinks --------------------------------------------------

#[test]
fn email_simple() {
    assert_eq!(
        parse_inlines("x <a@b.com>"),
        vec![t("x "), email("a@b.com")]
    );
}

#[test]
fn email_complex_local() {
    assert_eq!(
        parse_inlines("x <foo.bar+baz@example.co.uk>"),
        vec![t("x "), email("foo.bar+baz@example.co.uk")]
    );
}

// ----- HTML-as-text -----------------------------------------------------

#[test]
fn html_open_tag_is_text() {
    assert_eq!(parse_inlines("x <span>"), vec![t("x <span>")]);
}

#[test]
fn html_close_tag_is_text() {
    assert_eq!(parse_inlines("x </span>"), vec![t("x </span>")]);
}

#[test]
fn html_self_closing_is_text() {
    assert_eq!(parse_inlines("x <br/>"), vec![t("x <br/>")]);
}

#[test]
fn html_with_attributes_is_text() {
    assert_eq!(
        parse_inlines("x <a href=\"y\">"),
        vec![t("x <a href=\"y\">")]
    );
}

#[test]
fn html_comment_is_text() {
    assert_eq!(parse_inlines("x <!-- hi -->"), vec![t("x <!-- hi -->")]);
}

#[test]
fn lt_with_no_following_match_is_text() {
    assert_eq!(parse_inlines("a < b"), vec![t("a < b")]);
}

// ----- Priority --------------------------------------------------------

#[test]
fn nostr_inside_uri_autolink_stays_text() {
    // The autolink wins; nostr scheme bytes inside `<...>` are part of the
    // URL, not a separate NostrUri token.
    let s = "x <nostr:nevent1qzqzqzqzqzqzqzqzqzqzqzqzqzqzqzqzqzqzqz>";
    assert_eq!(parse_inlines(s), vec![t("x "), uri(&s[3..s.len() - 1])]);
}

#[test]
fn sensitive_uri_autolinks_are_preserved_and_classified() {
    for input in ["x <nostr:nsec1qqqqqq>", "x <web+nostr:nsec1qqqqqq>"] {
        let url = &input[3..input.len() - 1];
        assert_eq!(
            parse_inlines(input),
            vec![
                t("x "),
                Inline::Autolink {
                    url: url.to_owned(),
                    kind: AutolinkKind::Uri,
                    classification: LinkDestinationKind::Sensitive,
                },
            ]
        );
    }
}

#[test]
fn dangerous_uri_autolinks_are_preserved_and_classified() {
    for input in [
        "<javascript:alert(1)>",
        "<data:text/plain,payload>",
        "<vbscript:msgbox(1)>",
        "<file:///etc/passwd>",
    ] {
        let parsed = parse_inlines(input);
        let [
            Inline::Autolink {
                url,
                classification,
                ..
            },
        ] = parsed.as_slice()
        else {
            panic!("expected a classified autolink for {input}");
        };
        assert_eq!(url, &input[1..input.len() - 1]);
        assert_eq!(*classification, LinkDestinationKind::Dangerous);
    }
}

// ----- Bare URLs (GFM-style extended autolinks) ------------------------

#[test]
fn bare_www() {
    assert_eq!(
        parse_inlines("www.example.com/path"),
        vec![www("www.example.com/path")]
    );
}

#[test]
fn bare_www_strips_trailing_period() {
    assert_eq!(
        parse_inlines("See www.example.com/path."),
        vec![t("See "), www("www.example.com/path"), t(".")]
    );
}

#[test]
fn bare_https() {
    assert_eq!(
        parse_inlines("see https://example.com/path?q=1 ok"),
        vec![t("see "), uri("https://example.com/path?q=1"), t(" ok"),]
    );
}

#[test]
fn bare_http() {
    assert_eq!(
        parse_inlines("http://example.com"),
        vec![uri("http://example.com")]
    );
}

#[test]
fn bare_mailto() {
    assert_eq!(
        parse_inlines("ping mailto:foo@bar.com please"),
        vec![t("ping "), uri("mailto:foo@bar.com"), t(" please")]
    );
}

#[test]
fn bare_tel() {
    assert_eq!(
        parse_inlines("call tel:+15551234567 now"),
        vec![t("call "), uri("tel:+15551234567"), t(" now")]
    );
}

#[test]
fn bare_marmot() {
    assert_eq!(
        parse_inlines("open marmot://profile/npub1abc"),
        vec![t("open "), uri("marmot://profile/npub1abc")]
    );
}

#[test]
fn bare_whitenoise() {
    assert_eq!(
        parse_inlines("open whitenoise://group/abc"),
        vec![t("open "), uri("whitenoise://group/abc")]
    );
}

#[test]
fn bare_whitenoise_staging() {
    assert_eq!(
        parse_inlines("open whitenoise-staging://group/abc"),
        vec![t("open "), uri("whitenoise-staging://group/abc")]
    );
}

#[test]
fn whitenoise_opaque_form_stays_text() {
    // Only the authority form (`whitenoise://`) is recognized as a URL.
    // Opaque `whitenoise:foo` (no `//`) is not a known shape and stays literal.
    assert_eq!(parse_inlines("whitenoise:foo"), vec![t("whitenoise:foo")]);
}

#[test]
fn marmot_opaque_form_stays_text() {
    assert_eq!(parse_inlines("marmot:foo"), vec![t("marmot:foo")]);
}

#[test]
fn bare_url_strips_trailing_period() {
    assert_eq!(
        parse_inlines("Visit https://example.com."),
        vec![t("Visit "), uri("https://example.com"), t(".")]
    );
}

#[test]
fn bare_url_strips_trailing_punct_cluster() {
    assert_eq!(
        parse_inlines("really?! https://example.com?!"),
        vec![t("really?! "), uri("https://example.com"), t("?!")]
    );
}

#[test]
fn bare_url_paren_balanced_kept() {
    // Wikipedia-style: the URL contains `(` so the trailing `)` is part of it.
    let s = "see https://en.wikipedia.org/wiki/Foo_(bar) ok";
    assert_eq!(
        parse_inlines(s),
        vec![
            t("see "),
            uri("https://en.wikipedia.org/wiki/Foo_(bar)"),
            t(" ok"),
        ]
    );
}

#[test]
fn bare_url_paren_unbalanced_stripped() {
    // Parenthesized prose: trailing `)` belongs to the sentence, not the URL.
    assert_eq!(
        parse_inlines("(see https://example.com)"),
        vec![t("(see "), uri("https://example.com"), t(")")]
    );
}

#[test]
fn bare_url_inside_emphasis() {
    // The closing `*` must not be absorbed into the URL.
    assert_eq!(
        parse_inlines("*https://example.com*"),
        vec![Inline::Emph(vec![uri("https://example.com")])]
    );
}

#[test]
fn word_internal_https_rejected() {
    // "xhttps://" must not become a URL; the `h` is mid-word.
    assert_eq!(
        parse_inlines("xhttps://example.com"),
        vec![t("xhttps://example.com")]
    );
}

#[test]
fn bare_url_at_start_of_line() {
    assert_eq!(
        parse_inlines("https://example.com is the site"),
        vec![uri("https://example.com"), t(" is the site")]
    );
}

#[test]
fn multiple_bare_urls() {
    assert_eq!(
        parse_inlines("a https://x.com b mailto:y@z.io c"),
        vec![
            t("a "),
            uri("https://x.com"),
            t(" b "),
            uri("mailto:y@z.io"),
            t(" c"),
        ]
    );
}

#[test]
fn non_url_h_t_m_w_bytes_stay_text() {
    // Stress the bulk-scan tripwire: lots of h/m/t/w in prose with no URL.
    let s = "the warmth of his methods matters with whitespace";
    assert_eq!(parse_inlines(s), vec![t(s)]);
}

#[test]
fn malformed_scheme_falls_through_to_text() {
    // "http:/x" — single slash, not a recognized prefix; stays literal.
    assert_eq!(parse_inlines("http:/x"), vec![t("http:/x")]);
}

#[test]
fn bare_nostr_uri_still_structured() {
    // Bare `nostr:` URIs go through the existing NostrUri path, not the new
    // generic bare-URL autolink — `nostr:` is NOT in the bare-URL scheme list.
    let s = "x nostr:nevent1qzqzqzqzqzqzqzqzqzqzqzqzqzqzqzqzqzqzqz";
    let inlines = parse_inlines(s);
    assert_eq!(inlines.len(), 2);
    assert_eq!(inlines[0], t("x "));
    match &inlines[1] {
        Inline::NostrUri(e) => assert_eq!(e.hrp, NostrHrp::Nevent),
        other => panic!("expected NostrUri, got {other:?}"),
    }
}

#[test]
fn malformed_bare_nostr_stays_text() {
    // Per the documented design: `nostr:` without valid bech32 does NOT fall
    // back to a generic bare-URL autolink — it stays literal.
    assert_eq!(parse_inlines("nostr:foo"), vec![t("nostr:foo")]);
}

fn domain(label: &str, dest: &str) -> Inline {
    Inline::Link {
        dest: dest.to_owned(),
        title: None,
        children: vec![t(label)],
        classification: LinkDestinationKind::Web,
    }
}

#[test]
fn bare_domains_support_any_syntactic_ending() {
    for host in [
        "example.com",
        "example.chat",
        "example.network",
        "example.co.uk",
        "example.photography",
        "example.futureending",
        "example.rs",
        "example.md",
        "example.zip",
        "EXAMPLE.CHAT",
    ] {
        assert_eq!(
            parse_inlines(host),
            vec![domain(
                host,
                &format!("https://{}/", host.to_ascii_lowercase())
            )],
            "{host}"
        );
    }
}

#[test]
fn bare_domains_preserve_label_and_include_url_components() {
    let label = "sub.example.chat:8443/a_b?q=hello&other=2#part";
    assert_eq!(
        parse_inlines(&format!("See {label}.")),
        vec![
            t("See "),
            domain(label, &format!("https://{label}")),
            t(".")
        ]
    );
    assert_eq!(
        parse_inlines("(example.chat/wiki/Foo_(bar))."),
        vec![
            t("("),
            domain(
                "example.chat/wiki/Foo_(bar)",
                "https://example.chat/wiki/Foo_(bar)"
            ),
            t(").")
        ]
    );
    assert_eq!(
        parse_inlines("“example.chat”"),
        vec![
            t("“"),
            domain("example.chat", "https://example.chat/"),
            t("”")
        ]
    );
}

#[test]
fn bare_domains_normalize_international_hosts_without_changing_display() {
    for (label, dest) in [
        ("bücher.de", "https://xn--bcher-kva.de/"),
        ("例子.中国", "https://xn--fsqu00a.xn--fiqs8s/"),
        ("xn--bcher-kva.de", "https://xn--bcher-kva.de/"),
    ] {
        assert_eq!(parse_inlines(label), vec![domain(label, dest)]);
    }
}

#[test]
fn bare_domains_stay_inside_emphasis_but_outside_explicit_link_labels() {
    assert_eq!(
        parse_inlines("**example.chat**"),
        vec![Inline::Strong(vec![domain(
            "example.chat",
            "https://example.chat/"
        )])]
    );
    assert_eq!(
        parse_inlines("[example.chat](https://other.org)"),
        vec![domain("example.chat", "https://other.org")]
    );
    assert_eq!(
        parse_inlines("`example.chat`"),
        vec![Inline::Code("example.chat".into())]
    );
    assert_eq!(
        parse_inlines("$example.chat$"),
        vec![Inline::Math("example.chat".into())]
    );
}

#[test]
fn bare_domains_do_not_rescue_emails_paths_schemes_or_invalid_hosts() {
    for input in [
        "user@example.chat",
        "user.name@example.chat",
        "user:pass@example.chat",
        "user.name:pass@example.chat",
        "javascript:example.chat",
        "data:example.chat",
        "file://example.chat",
        "ftp://example.chat",
        "./example.chat",
        "src/example.chat",
        "example.chat\\file",
        "foo..example.chat",
        "-example.chat",
        "example-.chat",
        "example.c",
        "example.123",
        "1.2.3",
        "192.168.0.1",
        "example.chat_bad",
        "example.chat:abc",
        "example.chat:65536",
    ] {
        assert_eq!(parse_inlines(input), vec![t(input)], "{input}");
    }
}

#[test]
fn bare_domains_reject_oversized_hostnames_without_rescuing_suffixes() {
    let input = format!("{}.chat", "a".repeat(64));
    assert_eq!(parse_inlines(&input), vec![t(&input)]);
    let input = format!("{}.chat", "a".repeat(100_000));
    assert_eq!(parse_inlines(&input), vec![t(&input)]);
}

#[test]
fn bare_domain_paths_are_not_split_by_emphasis_delimiters() {
    assert_eq!(
        parse_inlines("*example.chat/a*b*c*"),
        vec![Inline::Emph(vec![domain(
            "example.chat/a*b*c",
            "https://example.chat/a*b*c"
        )])]
    );
    assert_eq!(
        parse_inlines("[example.chat]"),
        vec![
            t("["),
            domain("example.chat", "https://example.chat/"),
            t("]")
        ]
    );
}

#[test]
fn bare_domains_keep_multiple_sentence_links_separate() {
    assert_eq!(
        parse_inlines("example.chat,example.network"),
        vec![
            domain("example.chat", "https://example.chat/"),
            t(","),
            domain("example.network", "https://example.network/")
        ]
    );
}

#[test]
fn bare_domains_do_not_absorb_unicode_sentence_punctuation_or_emoji() {
    for tail in ["🎉", "—it works", "，谢谢", "！", "…"] {
        assert_eq!(
            parse_inlines(&format!("example.chat{tail}")),
            vec![domain("example.chat", "https://example.chat/"), t(tail)]
        );
    }
    for (open, close) in [("「", "」"), ("《", "》"), ("«", "»")] {
        assert_eq!(
            parse_inlines(&format!("{open}example.chat{close}")),
            vec![
                t(open),
                domain("example.chat", "https://example.chat/"),
                t(close)
            ]
        );
    }
    assert_eq!(
        parse_inlines("网站：example.chat"),
        vec![t("网站："), domain("example.chat", "https://example.chat/")]
    );
}

#[test]
fn bare_domains_accept_mixed_ascii_unicode_registered_idn_endings() {
    assert_eq!(
        parse_inlines("example.vermögensberatung"),
        vec![domain(
            "example.vermögensberatung",
            "https://example.xn--vermgensberatung-pwb/"
        )]
    );
}

#[test]
fn bare_domains_allow_formatting_around_ports_and_reject_intraword_openers() {
    assert_eq!(
        parse_inlines("**example.chat:8443**"),
        vec![Inline::Strong(vec![domain(
            "example.chat:8443",
            "https://example.chat:8443/"
        )])]
    );
    assert_eq!(
        parse_inlines("foo*example.chat"),
        vec![t("foo*example.chat")]
    );
    assert_eq!(parse_inlines("2*a.bc"), vec![t("2*a.bc")]);
}

#[test]
fn bare_domains_respect_escapes_and_do_not_link_truncated_unmatched_bracket_paths() {
    assert_eq!(parse_inlines(r"example\.chat"), vec![t("example.chat")]);
    assert_eq!(
        parse_inlines(r"example.chat\:8080"),
        vec![t("example.chat:8080")]
    );
    assert_eq!(
        parse_inlines("[ example.chat/a*b*c"),
        vec![t("[ example.chat/a"), Inline::Emph(vec![t("b")]), t("c")]
    );
}

#[test]
fn bare_domain_scanning_keeps_multibyte_boundaries_through_markdown() {
    for prefix in [
        "é ", "漢字 ", "🦀 ", "\\é ", "[é ", "&amp;é ", "`é` ", "é* ",
    ] {
        for suffix in [" 漢字", "🎉", "—tail", "\\é", "`漢字`", "*漢字*"] {
            // Slicing must remain valid after escapes, entities and multibyte text.
            let doc = marmot_markdown::parse(&format!("{prefix}bücher.de/a?q=1{suffix}"));
            serde_json::to_string(&doc).expect("serialize mixed UTF-8 source");
        }
    }
}

#[test]
fn bare_domains_bound_dns_length_after_idna_conversion() {
    let label = format!("{}.cn", vec!["中".repeat(12); 8].join("."));
    let host = format!("{}.cn", ["xn--fiqaaaaaaaaaaa"; 8].join("."));
    assert_eq!(
        parse_inlines(&label),
        vec![domain(&label, &format!("https://{host}/"))]
    );
    assert_eq!(
        parse_inlines("bu\u{308}cher.de"),
        vec![domain("bu\u{308}cher.de", "https://xn--bcher-kva.de/")]
    );
}

#[test]
fn bare_domain_paths_keep_unicode_symbols_and_slugs() {
    for (label, dest) in [
        ("example.chat/🦀", "https://example.chat/%F0%9F%A6%80"),
        (
            "example.chat/hello—world",
            "https://example.chat/hello%E2%80%94world",
        ),
    ] {
        assert_eq!(parse_inlines(label), vec![domain(label, dest)]);
    }
    assert_eq!(
        parse_inlines("example.chat\u{200b}evil"),
        vec![t("example.chat\u{200b}evil")]
    );
}

#[test]
fn bare_domains_keep_valid_apostrophes_and_balanced_brackets_in_url_tails() {
    for (label, dest) in [
        ("example.chat/it's", "https://example.chat/it's"),
        ("example.chat/?q[]=value", "https://example.chat/?q[]=value"),
        ("example.chat/path[part]", "https://example.chat/path[part]"),
    ] {
        assert_eq!(parse_inlines(label), vec![domain(label, dest)]);
    }
    assert_eq!(
        parse_inlines("'example.chat/it's'"),
        vec![
            t("'"),
            domain("example.chat/it's", "https://example.chat/it's"),
            t("'")
        ]
    );
    assert_eq!(
        parse_inlines("[example.chat/path]"),
        vec![
            t("["),
            domain("example.chat/path", "https://example.chat/path"),
            t("]")
        ]
    );
    assert_eq!(
        parse_inlines("<example.chat/path>"),
        vec![
            t("<"),
            domain("example.chat/path", "https://example.chat/path"),
            t(">")
        ]
    );
}

#[test]
fn bare_domain_paths_with_backslashes_are_not_linked_to_truncated_destinations() {
    assert_eq!(
        parse_inlines(r"example.chat/path\file"),
        vec![t(r"example.chat/path\file")]
    );
    assert_eq!(
        parse_inlines(r"example.chat/foo\_bar"),
        vec![t("example.chat/foo_bar")]
    );
}

#[test]
fn bare_domain_paths_leave_unicode_sentence_punctuation_outside_link() {
    for ending in ["。", "…", "：", "؟", "،", "।", "．", "｡"] {
        assert_eq!(
            parse_inlines(&format!("example.chat/docs{ending}")),
            vec![
                domain("example.chat/docs", "https://example.chat/docs"),
                t(ending)
            ]
        );
    }
}

#[test]
fn bare_domains_work_inside_underscore_emphasis_without_partial_hosts() {
    assert_eq!(
        parse_inlines("_example.chat_"),
        vec![Inline::Emph(vec![domain(
            "example.chat",
            "https://example.chat/"
        )])]
    );
    assert_eq!(
        parse_inlines("__example.chat__"),
        vec![Inline::Strong(vec![domain(
            "example.chat",
            "https://example.chat/"
        )])]
    );
    assert_eq!(
        parse_inlines("example.chat_bad"),
        vec![t("example.chat_bad")]
    );
}

#[test]
fn unmatched_bracket_fallback_preserves_word_and_non_text_boundaries() {
    assert_eq!(
        parse_inlines("[ foo*example.chat*"),
        vec![t("[ foo"), Inline::Emph(vec![t("example.chat")])]
    );
    assert_eq!(
        parse_inlines("[ `x`example.chat"),
        vec![t("[ "), Inline::Code("x".into()), t("example.chat")]
    );
    assert_eq!(
        parse_inlines("[ example.chat/a`b`"),
        vec![t("[ example.chat/a"), Inline::Code("b".into())]
    );
}

#[test]
fn repeated_failed_url_tails_remain_literal() {
    let direct = format!("{}\\", "a.bc/(".repeat(16_000));
    assert_eq!(parse_inlines(&direct), vec![t(&direct)]);
    let fallback = format!("[ {}*x*", "a.bc/,".repeat(16_000));
    assert_eq!(
        parse_inlines(&fallback),
        vec![
            t(&format!("[ {}", "a.bc/,".repeat(16_000))),
            Inline::Emph(vec![t("x")])
        ]
    );
}

#[test]
fn unmatched_bracket_format_containers_do_not_invent_shortened_paths() {
    for input in [
        "[ *example.chat/a*b* c",
        "[ **example.chat/a**b",
        "[ ~~example.chat/a~~b",
    ] {
        let doc = marmot_markdown::parse(input);
        let json = serde_json::to_string(&doc).unwrap();
        assert!(!json.contains("https://example.chat/a"), "{input}: {json}");
    }
}

#[test]
fn entity_encoded_domains_are_not_reinterpreted_by_bracket_recovery() {
    for (input, plain) in [
        ("example&#46;chat", "example.chat"),
        ("[ example&#46;chat", "[ example.chat"),
        ("[ example.chat&#47;x", "[ example.chat/x"),
    ] {
        assert_eq!(parse_inlines(input), vec![t(plain)]);
    }
}

#[test]
fn international_labels_are_not_shortened_at_script_transitions() {
    assert_eq!(
        parse_inlines("中文example.com"),
        vec![domain(
            "中文example.com",
            "https://xn--example-f43kr94o.com/"
        )]
    );
    assert_eq!(
        parse_inlines("example.com中文"),
        vec![domain(
            "example.com中文",
            "https://example.xn--com-x68do18h/"
        )]
    );
    assert_eq!(
        parse_inlines("中文abc.中国"),
        vec![domain(
            "中文abc.中国",
            "https://xn--abc-u68do18h.xn--fiqs8s/"
        )]
    );
}
