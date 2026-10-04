//! Bare web hosts share the ordinary Link DTO: original display text, HTTPS destination.
//!
//! Recognize raw candidates outside bracket labels before emphasis consumes URL
//! paths. A bounded plain-text fallback handles unmatched bracket prose without
//! reinterpreting code, images or explicit links. No DNS queries are performed.

use crate::ast::Inline;
use crate::destination::classify_link_destination;
use crate::inline::MAX_INLINE_NESTING_DEPTH;
use unicode_properties::{GeneralCategory, GeneralCategoryGroup, UnicodeGeneralCategory};

pub(crate) fn link_domains(
    items: &mut Vec<Inline>,
    depth: usize,
    initial_boundary: bool,
    container_end: bool,
) {
    // A generated Link plus its Text child must still fit the existing depth cap.
    if depth + 2 > MAX_INLINE_NESTING_DEPTH {
        return;
    }
    let mut linked = Vec::with_capacity(items.len());
    let mut originals = std::mem::take(items).into_iter().peekable();
    let mut boundary = initial_boundary;
    while let Some(mut item) = originals.next() {
        let next_boundary = match &item {
            Inline::Text(text) => left_boundary(text.chars().next_back()),
            Inline::SoftBreak | Inline::HardBreak => true,
            _ => false,
        };
        match item {
            Inline::Text(text) if text.contains(['.', '。', '．', '｡']) => {
                // A neighboring non-text node may have consumed URL path bytes.
                let followed_by_node = originals.peek().map_or(container_end, |next| {
                    !matches!(
                        next,
                        Inline::Text(_) | Inline::SoftBreak | Inline::HardBreak
                    )
                });
                link_text(text, &mut linked, boundary, followed_by_node);
                boundary = next_boundary;
                continue;
            }
            Inline::Emph(ref mut children)
            | Inline::Strong(ref mut children)
            | Inline::Strikethrough(ref mut children) => {
                // Closing formatting may have split a URL path. Conservatively
                // reject path tails that touch the end of this container.
                link_domains(children, depth + 1, boundary, true);
            }
            _ => {}
        }
        boundary = next_boundary;
        linked.push(item);
    }
    *items = linked;
}

fn link_text(text: String, out: &mut Vec<Inline>, initial_boundary: bool, followed_by_node: bool) {
    let mut i = 0;
    let mut plain_start = 0;
    let mut previous = if initial_boundary { None } else { Some('a') };
    let mut tails = TailScanner::default();
    while i < text.len() {
        let ch = text[i..].chars().next().unwrap();
        if ch.is_alphanumeric()
            && left_boundary(previous)
            && let Some((dest, end)) = try_domain(&text, i, &mut tails, followed_by_node)
        {
            if plain_start < i {
                out.push(Inline::Text(text[plain_start..i].to_owned()));
            }
            out.push(Inline::Link {
                classification: classify_link_destination(&dest),
                dest,
                title: None,
                children: vec![Inline::Text(text[i..end].to_owned())],
            });
            i = end;
            plain_start = end;
            previous = text[..end].chars().next_back();
            continue;
        }
        previous = Some(ch);
        i += ch.len_utf8();
    }
    if plain_start == 0 {
        out.push(Inline::Text(text));
    } else if plain_start < text.len() {
        out.push(Inline::Text(text[plain_start..].to_owned()));
    }
}

/// Each scanner belongs to one text buffer. Monotonically advancing probes
/// reuse the same stop, including failures, instead of rescanning long tails.
#[derive(Default)]
pub(crate) struct TailScanner {
    range: Option<(usize, usize, bool)>,
    #[cfg(test)]
    scanned_bytes: usize,
}

impl TailScanner {
    fn scan(&mut self, text: &str, start: usize) -> (usize, bool) {
        if let Some((from, end, backslash)) = self.range
            && from <= start
            && start <= end
        {
            return (end, backslash);
        }
        let mut stop = text.len();
        let mut backslash = false;
        for (offset, ch) in text[start..].char_indices() {
            #[cfg(test)]
            {
                self.scanned_bytes += ch.len_utf8();
            }
            if tail_end(ch) {
                stop = start + offset;
                backslash = ch == '\\';
                break;
            }
        }
        self.range = Some((start, stop, backslash));
        (stop, backslash)
    }
}

pub(crate) fn candidate_start(text: &str, i: usize) -> bool {
    text[i..].chars().next().is_some_and(char::is_alphanumeric)
        && left_boundary(text[..i].chars().next_back())
}

pub(crate) fn left_boundary(previous: Option<char>) -> bool {
    previous.is_none_or(|ch| {
        ch.is_whitespace()
            || matches!(
                ch,
                '(' | '['
                    | '{'
                    | '<'
                    | ')'
                    | ']'
                    | '}'
                    | '\''
                    | '"'
                    | ','
                    | ';'
                    | '!'
                    | '?'
                    | '，'
                    | '；'
                    | '！'
                    | '？'
                    | '、'
                    | '：'
            )
            || (!ch.is_ascii()
                && matches!(
                    ch.general_category(),
                    GeneralCategory::OpenPunctuation | GeneralCategory::InitialPunctuation
                ))
    })
}

fn host_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric()
        || matches!(ch, '-' | '.' | '。' | '．' | '｡' | '\u{200c}' | '\u{200d}')
        || (!ch.is_ascii()
            && matches!(
                ch.general_category_group(),
                GeneralCategoryGroup::Letter
                    | GeneralCategoryGroup::Mark
                    | GeneralCategoryGroup::Number
            ))
}

fn tail_end(ch: char) -> bool {
    ch.is_whitespace()
        || ch.is_control()
        || matches!(
            ch,
            '<' | '>' | '"' | '`' | '\\' | '，' | '；' | '！' | '？' | '、'
        )
        || (!ch.is_ascii()
            && matches!(
                ch.general_category(),
                GeneralCategory::OpenPunctuation
                    | GeneralCategory::ClosePunctuation
                    | GeneralCategory::InitialPunctuation
                    | GeneralCategory::FinalPunctuation
            ))
}

pub(crate) fn try_domain(
    text: &str,
    start: usize,
    tails: &mut TailScanner,
    reject_tail_at_end: bool,
) -> Option<(String, usize)> {
    // Bound all failed hostname probes. A rejected token cannot be rescued at
    // an internal dot/hyphen because those are not left boundaries.
    let mut host_end = start;
    for (offset, ch) in text[start..].char_indices() {
        if !host_char(ch) {
            break;
        }
        host_end = start + offset + ch.len_utf8();
        if host_end - start > 253 * 4 {
            return None;
        }
    }
    if text[host_end..].chars().next().is_some_and(|ch| {
        !ch.is_whitespace()
            && !ch.is_control()
            && ch.general_category_group() == GeneralCategoryGroup::Other
    }) {
        return None;
    }
    let raw_host = text[start..host_end].trim_end_matches(['.', '。', '．', '｡']);
    if !raw_host.contains(['.', '。', '．', '｡']) {
        return None;
    }
    // Do not turn the dotted local part of a bare email into a web link.
    // Bound this probe too, and stop at URL path/query/fragment boundaries.
    if text[start..]
        .chars()
        .take(254)
        .take_while(|ch| {
            !ch.is_whitespace()
                && !ch.is_control()
                && !matches!(
                    ch,
                    '/' | '?'
                        | '#'
                        | ','
                        | ';'
                        | '('
                        | ')'
                        | '['
                        | ']'
                        | '<'
                        | '>'
                        | '"'
                        | '‘'
                        | '’'
                        | '“'
                        | '”'
                )
        })
        .any(|ch| ch == '@')
    {
        return None;
    }
    // IDNA conversion is delegated to the already shared URL implementation.
    let url::Host::Domain(host) = url::Host::parse(raw_host).ok()? else {
        return None; // IPv4/IPv6 and version-like numeric tokens are not bare domains.
    };
    if host.len() > 253 {
        return None;
    }
    for label in host.split('.') {
        if label.is_empty()
            || label.len() > 63
            || label.starts_with('-')
            || label.ends_with('-')
            || !label
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return None;
        }
    }
    let ending = host.rsplit('.').next()?;
    if !(ending.len() >= 2 && ending.bytes().all(|b| b.is_ascii_alphabetic())
        || ending.starts_with("xn--"))
    {
        return None;
    }

    let mut end = start + raw_host.len();
    // Trailing host dots are sentence punctuation, never a partial match of
    // a malformed host followed by a path, port, or credentials.
    if end < host_end {
        if text[host_end..].starts_with(['/', '?', '#', ':', '@', '_', '\\', '%']) {
            return None;
        }
    } else {
        if text[end..].starts_with(['@', '\\', '%', '+', '=', '$', '&', '^', '|']) {
            return None;
        }
        if text[end..].starts_with('_') {
            let remainder = text[end..].trim_start_matches('_');
            if remainder.chars().next().is_some_and(|ch| {
                ch.is_alphanumeric() || matches!(ch, '-' | '.' | '/' | '?' | '#' | '@')
            }) {
                return None;
            }
        }
        if text[end..].starts_with(':') {
            let digits_start = end + 1;
            let digits_len = text[digits_start..]
                .bytes()
                .take_while(u8::is_ascii_digit)
                .count();
            if digits_len == 0 {
                // A colon ending a sentence is punctuation; a malformed port
                // stays literal instead of launching a different destination.
                if text[digits_start..]
                    .chars()
                    .next()
                    .is_some_and(|ch| !tail_end(ch))
                {
                    return None;
                }
            } else {
                let port_end = digits_start + digits_len;
                text[digits_start..port_end].parse::<u16>().ok()?;
                end = port_end;
                if text[end..].chars().next().is_some_and(|ch| {
                    !tail_end(ch)
                        && !matches!(
                            ch,
                            '/' | '?'
                                | '#'
                                | '.'
                                | ','
                                | ';'
                                | '!'
                                | ')'
                                | '}'
                                | '*'
                                | '_'
                                | '~'
                                | ':'
                                | '\''
                        )
                }) {
                    return None;
                }
            }
        }
        if text[end..].starts_with(['/', '?', '#']) {
            let (stop, backslash) = tails.scan(text, end);
            // Reject before trimming or URL parsing: every later candidate in
            // this range must reuse the same failure without expensive work.
            if backslash || (reject_tail_at_end && stop == text.len()) {
                return None;
            }
            end = stop;
            end = trim_domain_tail(text, start, end);
        }
    }
    let destination = url::Url::parse(&format!("https://{}", &text[start..end])).ok()?;
    // Require the validated authority, even if URL syntax would normalize a
    // backslash, userinfo or another authority into a different host.
    if destination.host_str() != Some(host.as_str())
        || !destination.username().is_empty()
        || destination.password().is_some()
    {
        return None;
    }
    Some((destination.into(), end))
}

/// One scan and one trim walk, including mixed unmatched closers. Rescanning
/// after each closing bracket would make a hostile punctuation suffix quadratic.
fn trim_domain_tail(text: &str, start: usize, mut end: usize) -> usize {
    let bytes = text.as_bytes();
    let mut opens = [0usize; 3];
    let mut closes = [0usize; 3];
    for &b in &bytes[start..end] {
        match b {
            b'(' => opens[0] += 1,
            b')' => closes[0] += 1,
            b'[' => opens[1] += 1,
            b']' => closes[1] += 1,
            b'{' => opens[2] += 1,
            b'}' => closes[2] += 1,
            _ => {}
        }
    }
    let mut quoted = text[..start].ends_with('\'');
    while end > start {
        let last = text[start..end].chars().next_back().unwrap();
        if !last.is_ascii() && last.general_category() == GeneralCategory::OtherPunctuation {
            end -= last.len_utf8();
            continue;
        }
        match bytes[end - 1] {
            b'.' | b',' | b';' | b':' | b'!' | b'?' | b'*' | b'_' | b'~' => end -= 1,
            b'\'' if quoted => {
                end -= 1;
                quoted = false;
            }
            b')' | b']' | b'}' => {
                let index = match bytes[end - 1] {
                    b')' => 0,
                    b']' => 1,
                    _ => 2,
                };
                if closes[index] <= opens[index] {
                    break;
                }
                closes[index] -= 1;
                end -= 1;
            }
            _ => break,
        }
    }
    end
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_tail_probes_scan_each_range_once() {
        for (text, reject_at_end) in [
            (format!("{}\\", "a.bc/(".repeat(16_000)), false),
            ("a.bc/,".repeat(16_000), true),
        ] {
            let mut scanner = TailScanner::default();
            for start in (0..96_000).step_by(6) {
                assert!(try_domain(&text, start, &mut scanner, reject_at_end).is_none());
            }
            assert!(scanner.scanned_bytes <= text.len());
        }
    }
}
