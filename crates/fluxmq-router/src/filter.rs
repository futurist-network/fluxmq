//! Filter DSL: parse / render / evaluate. Grammar and comparison rules are documented in
//! `fluxmq_proto::routing`. Pure functions — no I/O.
//!
//! ## Lexical details (beyond the contract grammar)
//!
//! * Keywords are case-insensitive: `AND`, `IN`, `NOT IN`, `EXISTS`, `NOT EXISTS`, `PREFIX`,
//!   `CONTAINS`, `true`, `false`, `null`. `OR` is rejected with a dedicated error.
//! * `==` is accepted for `=`, `<>` for `!=`; `( … )` for `[ … ]` after `IN`.
//! * Strings: `"…"` or `'…'` with escapes `\" \' \\ \/ \n \r \t \b \f \uXXXX`.
//! * Numbers: JSON numbers incl. negatives, decimals and exponents (`-12.5e3`). Integers keep
//!   integer precision (i64/u64), everything else is f64.
//! * Bare words in value position are strings (`country = MD`, `type = order.created`).
//! * Fields: `key`, `partition`, `header.<name>`, `payload.<path>` (explicit payload path,
//!   needed when the first path segment is `key`/`partition`/`header`/`payload`) or any other
//!   bare path. Names with unusual characters are quoted: `header."x y"`, `payload."a b"`, or
//!   a quoted field (`"a b" = 1`, always a payload path).
//!
//! ## Evaluation details
//!
//! * A field that is absent never satisfies `=`, `>`, `>=`, `<`, `<=`, `in`, `prefix`,
//!   `contains`, `exists`; it satisfies `!=`, `not in` and `not exists` (they are negations).
//! * Payload conditions never match a payload that is not valid JSON (any op).
//! * `exists` is true for a present JSON `null`.
//! * `contains`: substring for strings; element equality for JSON arrays (`tags contains vip`).
//! * `prefix`/`contains` compare the textual form of numbers/bools.
//! * Header names are matched exactly first, then case-insensitively.

use std::borrow::Cow;
use std::cell::OnceCell;
use std::cmp::Ordering;

use fluxmq_proto::Message;
use fluxmq_proto::routing::{Condition, FieldRef, Op};
use serde_json::{Number, Value};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("filter syntax error at {position}: {message}")]
pub struct FilterError {
    /// Byte offset into the input.
    pub position: usize,
    pub message: String,
}

impl FilterError {
    fn new(position: usize, message: impl Into<String>) -> Self {
        Self {
            position,
            message: message.into(),
        }
    }
}

pub fn parse(input: &str) -> Result<Vec<Condition>, FilterError> {
    let tokens = lex(input)?;
    Parser {
        tokens,
        pos: 0,
        input_len: input.len(),
    }
    .parse()
}

/// Canonical DSL text; `parse(render(c)) == c` for every condition list that passes route
/// validation (scalar values, lists only after `in`/`not in`, no value on `exists`).
pub fn render(conditions: &[Condition]) -> String {
    let mut out = String::new();
    for (i, c) in conditions.iter().enumerate() {
        if i > 0 {
            out.push_str(" AND ");
        }
        render_field(&c.field, &mut out);
        let op = match c.op {
            Op::Eq => "=",
            Op::Ne => "!=",
            Op::Gt => ">",
            Op::Gte => ">=",
            Op::Lt => "<",
            Op::Lte => "<=",
            Op::In => "in",
            Op::NotIn => "not in",
            Op::Prefix => "prefix",
            Op::Contains => "contains",
            Op::Exists => "exists",
            Op::NotExists => "not exists",
        };
        out.push(' ');
        out.push_str(op);
        if !matches!(c.op, Op::Exists | Op::NotExists) {
            out.push(' ');
            match &c.value {
                Some(v) if matches!(c.op, Op::In | Op::NotIn) && !v.is_array() => {
                    out.push('[');
                    render_value(v, &mut out);
                    out.push(']');
                }
                Some(v) => render_value(v, &mut out),
                None => out.push_str("null"),
            }
        }
    }
    out
}

/// All conditions must hold. The JSON payload is parsed at most once per call.
pub fn matches(conditions: &[Condition], message: &Message) -> bool {
    matches_in(conditions, &MatchContext::new(message))
}

/// A message prepared for evaluation against several condition lists (one router pass over
/// many routes): the JSON payload is parsed lazily, at most once per context.
pub struct MatchContext<'a> {
    message: &'a Message,
    doc: OnceCell<Option<Value>>,
}

impl<'a> MatchContext<'a> {
    pub fn new(message: &'a Message) -> Self {
        Self {
            message,
            doc: OnceCell::new(),
        }
    }

    pub fn message(&self) -> &'a Message {
        self.message
    }

    fn doc(&self) -> Option<&Value> {
        self.doc
            .get_or_init(|| serde_json::from_slice(&self.message.payload).ok())
            .as_ref()
    }
}

/// [`matches`] with a shared [`MatchContext`]. Cheap (key/header/partition) conditions are
/// evaluated first so that the payload is only parsed when they all pass.
pub fn matches_in(conditions: &[Condition], ctx: &MatchContext<'_>) -> bool {
    let is_payload = |c: &&Condition| matches!(c.field, FieldRef::Payload(_));
    conditions
        .iter()
        .filter(|c| !is_payload(c))
        .all(|c| eval(c, ctx))
        && conditions.iter().filter(is_payload).all(|c| eval(c, ctx))
}

// ---------------------------------------------------------------------------------------------
// Lexer

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    /// A run of identifier characters (raw text).
    Word(String),
    /// A number literal (raw text kept for field position).
    Num(Number, String),
    Str(String),
    Sym(&'static str),
    Eof,
}

#[derive(Debug, Clone)]
struct Token {
    tok: Tok,
    start: usize,
    end: usize,
}

fn is_ident_start(c: char) -> bool {
    c.is_alphanumeric() || matches!(c, '_' | '-' | '.' | '$' | '@' | '/' | ':')
}

fn is_ident_char(c: char) -> bool {
    is_ident_start(c) || c == '+'
}

fn lex(input: &str) -> Result<Vec<Token>, FilterError> {
    let mut out = Vec::new();
    let mut it = input.char_indices().peekable();
    while let Some(&(i, c)) = it.peek() {
        if c.is_whitespace() {
            it.next();
            continue;
        }
        let sym2 = input.get(i..i + 2);
        let (tok, len) = match c {
            '"' | '\'' => {
                let (s, end) = lex_string(input, i, c)?;
                // Advance the iterator to `end`.
                while it.peek().is_some_and(|&(j, _)| j < end) {
                    it.next();
                }
                out.push(Token {
                    tok: Tok::Str(s),
                    start: i,
                    end,
                });
                continue;
            }
            '!' if sym2 == Some("!=") => (Tok::Sym("!="), 2),
            '!' => {
                return Err(FilterError::new(i, "unexpected '!' (did you mean '!='?)"));
            }
            '=' if sym2 == Some("==") => (Tok::Sym("="), 2),
            '=' => (Tok::Sym("="), 1),
            '<' if sym2 == Some("<=") => (Tok::Sym("<="), 2),
            '<' if sym2 == Some("<>") => (Tok::Sym("!="), 2),
            '<' => (Tok::Sym("<"), 1),
            '>' if sym2 == Some(">=") => (Tok::Sym(">="), 2),
            '>' => (Tok::Sym(">"), 1),
            '[' => (Tok::Sym("["), 1),
            ']' => (Tok::Sym("]"), 1),
            '(' => (Tok::Sym("("), 1),
            ')' => (Tok::Sym(")"), 1),
            ',' => (Tok::Sym(","), 1),
            c if is_ident_start(c) => {
                let mut end = i;
                while let Some(&(j, ch)) = it.peek() {
                    if is_ident_char(ch) {
                        end = j + ch.len_utf8();
                        it.next();
                    } else {
                        break;
                    }
                }
                let raw = &input[i..end];
                let tok = match parse_number(raw) {
                    Some(n) => Tok::Num(n, raw.to_string()),
                    None => Tok::Word(raw.to_string()),
                };
                out.push(Token { tok, start: i, end });
                continue;
            }
            other => {
                return Err(FilterError::new(
                    i,
                    format!("unexpected character '{other}'"),
                ));
            }
        };
        for _ in 0..len {
            it.next();
        }
        out.push(Token {
            tok,
            start: i,
            end: i + len,
        });
    }
    out.push(Token {
        tok: Tok::Eof,
        start: input.len(),
        end: input.len(),
    });
    Ok(out)
}

/// Returns the unescaped string and the byte offset just past the closing quote.
fn lex_string(input: &str, start: usize, quote: char) -> Result<(String, usize), FilterError> {
    let mut s = String::new();
    let mut chars = input[start + 1..].char_indices();
    while let Some((off, c)) = chars.next() {
        let at = start + 1 + off;
        if c == quote {
            return Ok((s, at + 1));
        }
        if c != '\\' {
            s.push(c);
            continue;
        }
        let Some((_, e)) = chars.next() else {
            break;
        };
        match e {
            '"' | '\'' | '\\' | '/' => s.push(e),
            'n' => s.push('\n'),
            'r' => s.push('\r'),
            't' => s.push('\t'),
            'b' => s.push('\u{8}'),
            'f' => s.push('\u{c}'),
            'u' => {
                let hex = |chars: &mut std::str::CharIndices<'_>| -> Option<u32> {
                    let mut v = 0u32;
                    for _ in 0..4 {
                        v = v * 16 + chars.next()?.1.to_digit(16)?;
                    }
                    Some(v)
                };
                let bad = || FilterError::new(at, "invalid \\u escape (expected 4 hex digits)");
                let hi = hex(&mut chars).ok_or_else(bad)?;
                let code = if (0xD800..0xDC00).contains(&hi) {
                    // Surrogate pair: expect `\uDC00-DFFF` next.
                    let lo = match (chars.next(), chars.next()) {
                        (Some((_, '\\')), Some((_, 'u'))) => hex(&mut chars).ok_or_else(bad)?,
                        _ => return Err(bad()),
                    };
                    if !(0xDC00..0xE000).contains(&lo) {
                        return Err(bad());
                    }
                    0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00)
                } else {
                    hi
                };
                s.push(char::from_u32(code).ok_or_else(bad)?);
            }
            other => {
                return Err(FilterError::new(
                    at,
                    format!("invalid escape '\\{other}' in string"),
                ));
            }
        }
    }
    Err(FilterError::new(start, "unterminated string"))
}

/// JSON number grammar: `-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?`.
fn parse_number(raw: &str) -> Option<Number> {
    let b = raw.as_bytes();
    let mut i = 0;
    if b.first() == Some(&b'-') {
        i += 1;
    }
    let digits = |i: &mut usize| {
        let s = *i;
        while *i < b.len() && b[*i].is_ascii_digit() {
            *i += 1;
        }
        *i - s
    };
    let int_start = i;
    let n = digits(&mut i);
    if n == 0 || (n > 1 && b[int_start] == b'0') {
        return None;
    }
    let mut is_int = true;
    if i < b.len() && b[i] == b'.' {
        i += 1;
        is_int = false;
        if digits(&mut i) == 0 {
            return None;
        }
    }
    if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
        i += 1;
        is_int = false;
        if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
            i += 1;
        }
        if digits(&mut i) == 0 {
            return None;
        }
    }
    if i != b.len() {
        return None;
    }
    if is_int {
        if let Ok(v) = raw.parse::<i64>() {
            return Some(Number::from(v));
        }
        if let Ok(v) = raw.parse::<u64>() {
            return Some(Number::from(v));
        }
    }
    raw.parse::<f64>().ok().and_then(Number::from_f64)
}

// ---------------------------------------------------------------------------------------------
// Parser

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    input_len: usize,
}

fn word_is(t: &Tok, kw: &str) -> bool {
    matches!(t, Tok::Word(w) if w.eq_ignore_ascii_case(kw))
}

fn describe(t: &Tok) -> String {
    match t {
        Tok::Word(w) => format!("'{w}'"),
        Tok::Num(_, raw) => format!("'{raw}'"),
        Tok::Str(s) => format!("string \"{s}\""),
        Tok::Sym(s) => format!("'{s}'"),
        Tok::Eof => "end of filter".to_string(),
    }
}

impl Parser {
    fn peek(&self) -> &Token {
        &self.tokens[self.pos.min(self.tokens.len() - 1)]
    }

    fn next(&mut self) -> Token {
        let t = self.peek().clone();
        if self.pos < self.tokens.len() - 1 {
            self.pos += 1;
        }
        t
    }

    fn parse(mut self) -> Result<Vec<Condition>, FilterError> {
        let mut out = Vec::new();
        if self.peek().tok == Tok::Eof {
            return Ok(out);
        }
        loop {
            out.push(self.condition()?);
            let t = self.next();
            match &t.tok {
                Tok::Eof => return Ok(out),
                w if word_is(w, "and") => {
                    if self.peek().tok == Tok::Eof {
                        return Err(FilterError::new(
                            self.input_len,
                            "expected a condition after AND",
                        ));
                    }
                }
                w if word_is(w, "or") => {
                    return Err(FilterError::new(
                        t.start,
                        "OR is not supported: all conditions are combined with AND \
                         (use several routes or searches instead)",
                    ));
                }
                other => {
                    return Err(FilterError::new(
                        t.start,
                        format!("expected AND or end of filter, found {}", describe(other)),
                    ));
                }
            }
        }
    }

    fn condition(&mut self) -> Result<Condition, FilterError> {
        let field = self.field()?;
        let t = self.next();
        let op = match &t.tok {
            Tok::Sym("=") => Op::Eq,
            Tok::Sym("!=") => Op::Ne,
            Tok::Sym(">") => Op::Gt,
            Tok::Sym(">=") => Op::Gte,
            Tok::Sym("<") => Op::Lt,
            Tok::Sym("<=") => Op::Lte,
            w if word_is(w, "in") => Op::In,
            w if word_is(w, "prefix") => Op::Prefix,
            w if word_is(w, "contains") => Op::Contains,
            w if word_is(w, "exists") => Op::Exists,
            w if word_is(w, "not") => {
                let n = self.next();
                if word_is(&n.tok, "in") {
                    Op::NotIn
                } else if word_is(&n.tok, "exists") {
                    Op::NotExists
                } else {
                    return Err(FilterError::new(
                        n.start,
                        format!(
                            "expected IN or EXISTS after NOT, found {}",
                            describe(&n.tok)
                        ),
                    ));
                }
            }
            other => {
                return Err(FilterError::new(
                    t.start,
                    format!(
                        "expected an operator (=, !=, >, >=, <, <=, in, not in, prefix, \
                         contains, exists, not exists), found {}",
                        describe(other)
                    ),
                ));
            }
        };
        let value = match op {
            Op::Exists | Op::NotExists => None,
            Op::In | Op::NotIn => Some(self.list()?),
            _ => Some(self.scalar()?),
        };
        Ok(Condition { field, op, value })
    }

    fn field(&mut self) -> Result<FieldRef, FilterError> {
        let t = self.next();
        let quoted_after = |p: &mut Self, prefix_end: usize, what: &str| {
            let n = p.peek().clone();
            match n.tok {
                Tok::Str(s) if n.start == prefix_end => {
                    p.next();
                    Ok(s)
                }
                _ => Err(FilterError::new(
                    prefix_end,
                    format!("expected {what} name"),
                )),
            }
        };
        match t.tok {
            Tok::Word(w) => {
                let lower = w.to_ascii_lowercase();
                if lower == "key" {
                    Ok(FieldRef::Key)
                } else if lower == "partition" {
                    Ok(FieldRef::Partition)
                } else if lower.starts_with("header.") {
                    let rest = &w["header.".len()..];
                    if rest.is_empty() {
                        quoted_after(self, t.end, "header").map(FieldRef::Header)
                    } else {
                        Ok(FieldRef::Header(rest.to_string()))
                    }
                } else if lower.starts_with("payload.") {
                    let rest = &w["payload.".len()..];
                    if rest.is_empty() {
                        quoted_after(self, t.end, "payload path").map(FieldRef::Payload)
                    } else {
                        Ok(FieldRef::Payload(rest.to_string()))
                    }
                } else if matches!(lower.as_str(), "and" | "or") {
                    Err(FilterError::new(
                        t.start,
                        format!("expected a field, found '{w}'"),
                    ))
                } else {
                    Ok(FieldRef::Payload(w))
                }
            }
            Tok::Num(_, raw) => Ok(FieldRef::Payload(raw)),
            Tok::Str(s) => Ok(FieldRef::Payload(s)),
            other => Err(FilterError::new(
                t.start,
                format!(
                    "expected a field (key, partition, header.<name> or a payload path), \
                     found {}",
                    describe(&other)
                ),
            )),
        }
    }

    fn scalar(&mut self) -> Result<Value, FilterError> {
        let t = self.next();
        match t.tok {
            Tok::Num(n, _) => Ok(Value::Number(n)),
            Tok::Str(s) => Ok(Value::String(s)),
            Tok::Word(w) => {
                let lower = w.to_ascii_lowercase();
                match lower.as_str() {
                    "true" => Ok(Value::Bool(true)),
                    "false" => Ok(Value::Bool(false)),
                    "null" => Ok(Value::Null),
                    "and" | "or" => Err(FilterError::new(
                        t.start,
                        format!("expected a value, found '{w}' (quote it to use it as text)"),
                    )),
                    _ => Ok(Value::String(w)),
                }
            }
            Tok::Sym("[") | Tok::Sym("(") => Err(FilterError::new(
                t.start,
                "lists are only allowed after IN / NOT IN",
            )),
            other => Err(FilterError::new(
                t.start,
                format!("expected a value, found {}", describe(&other)),
            )),
        }
    }

    fn list(&mut self) -> Result<Value, FilterError> {
        let t = self.next();
        let close = match t.tok {
            Tok::Sym("[") => "]",
            Tok::Sym("(") => ")",
            other => {
                return Err(FilterError::new(
                    t.start,
                    format!("expected '[' to start a list, found {}", describe(&other)),
                ));
            }
        };
        let mut items = Vec::new();
        loop {
            items.push(self.scalar()?);
            let t = self.next();
            match t.tok {
                Tok::Sym(",") => {}
                Tok::Sym(s) if s == close => return Ok(Value::Array(items)),
                other => {
                    return Err(FilterError::new(
                        t.start,
                        format!("expected ',' or '{close}', found {}", describe(&other)),
                    ));
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Render

fn is_bare(s: &str) -> bool {
    let mut chars = s.chars();
    chars.next().is_some_and(is_ident_start) && chars.all(is_ident_char)
}

fn render_field(f: &FieldRef, out: &mut String) {
    match f {
        FieldRef::Key => out.push_str("key"),
        FieldRef::Partition => out.push_str("partition"),
        FieldRef::Header(n) => {
            out.push_str("header.");
            if is_bare(n) {
                out.push_str(n);
            } else {
                render_string(n, out);
            }
        }
        FieldRef::Payload(p) => {
            let lower = p.to_ascii_lowercase();
            let reserved = matches!(
                lower.as_str(),
                "key" | "partition" | "and" | "or" | "header." | "payload."
            ) || lower.starts_with("header.")
                || lower.starts_with("payload.");
            if is_bare(p) && !reserved {
                out.push_str(p);
            } else {
                out.push_str("payload.");
                if is_bare(p) {
                    out.push_str(p);
                } else {
                    render_string(p, out);
                }
            }
        }
    }
}

fn render_string(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

fn render_value(v: &Value, out: &mut String) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => out.push_str(&n.to_string()),
        Value::String(s) => render_string(s, out),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                render_value(item, out);
            }
            out.push(']');
        }
        // Not expressible in the DSL (rejected by route validation): best effort.
        Value::Object(_) => render_string(&v.to_string(), out),
    }
}

// ---------------------------------------------------------------------------------------------
// Evaluation

/// A borrowed view of a field value or literal.
#[derive(Debug, Clone)]
enum Val<'a> {
    Null,
    Bool(bool),
    Num(Cow<'a, Number>),
    Str(&'a str),
    Array(&'a [Value]),
    Object(&'a Value),
}

impl<'a> Val<'a> {
    fn of(v: &'a Value) -> Self {
        match v {
            Value::Null => Val::Null,
            Value::Bool(b) => Val::Bool(*b),
            Value::Number(n) => Val::Num(Cow::Borrowed(n)),
            Value::String(s) => Val::Str(s),
            Value::Array(a) => Val::Array(a),
            Value::Object(_) => Val::Object(v),
        }
    }

    /// Textual form of scalars for `prefix`/`contains` and string ↔ scalar equality.
    fn text(&self) -> Option<Cow<'a, str>> {
        match self {
            Val::Str(s) => Some(Cow::Borrowed(s)),
            Val::Num(n) => Some(Cow::Owned(n.to_string())),
            Val::Bool(b) => Some(Cow::Borrowed(if *b { "true" } else { "false" })),
            Val::Null => Some(Cow::Borrowed("null")),
            _ => None,
        }
    }
}

/// Parses a numeric string with the JSON number grammar (no whitespace, no `inf`/`NaN`).
fn numeric_str(s: &str) -> Option<Number> {
    parse_number(s)
}

fn num_cmp(a: &Number, b: &Number) -> Option<Ordering> {
    if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
        return Some(x.cmp(&y));
    }
    if let (Some(x), Some(y)) = (a.as_u64(), b.as_u64()) {
        return Some(x.cmp(&y));
    }
    // Mixed signs with a u64 beyond i64: sign decides.
    match (a.as_u64(), b.as_i64()) {
        (Some(_), Some(y)) if y < 0 => return Some(Ordering::Greater),
        _ => {}
    }
    match (a.as_i64(), b.as_u64()) {
        (Some(x), Some(_)) if x < 0 => return Some(Ordering::Less),
        _ => {}
    }
    a.as_f64()?.partial_cmp(&b.as_f64()?)
}

fn val_eq(a: &Val<'_>, b: &Val<'_>) -> bool {
    match (a, b) {
        (Val::Num(x), Val::Num(y)) => num_cmp(x, y) == Some(Ordering::Equal),
        (Val::Str(x), Val::Str(y)) => x == y,
        (Val::Bool(x), Val::Bool(y)) => x == y,
        (Val::Null, Val::Null) => true,
        // Number ↔ numeric string (`order_id = 8842` matches "8842").
        (Val::Num(n), Val::Str(s)) | (Val::Str(s), Val::Num(n)) => {
            numeric_str(s).is_some_and(|m| num_cmp(n, &m) == Some(Ordering::Equal))
        }
        // Headers/keys are strings: `header.x = true` matches "true".
        (Val::Str(s), other @ (Val::Bool(_) | Val::Null))
        | (other @ (Val::Bool(_) | Val::Null), Val::Str(s)) => {
            other.text().is_some_and(|t| t == *s)
        }
        (Val::Array(x), Val::Array(y)) => x == y,
        (Val::Object(x), Val::Object(y)) => x == y,
        _ => false,
    }
}

fn val_cmp(a: &Val<'_>, b: &Val<'_>) -> Option<Ordering> {
    match (a, b) {
        (Val::Num(x), Val::Num(y)) => num_cmp(x, y),
        (Val::Str(x), Val::Str(y)) => Some(x.cmp(y)),
        (Val::Num(n), Val::Str(s)) => num_cmp(n, &numeric_str(s)?),
        (Val::Str(s), Val::Num(n)) => num_cmp(&numeric_str(s)?, n),
        _ => None,
    }
}

/// Resolves the field. Outer `None`: the payload is not JSON (payload conditions never match).
/// Inner `None`: the field is absent.
fn resolve<'a>(field: &FieldRef, ctx: &'a MatchContext<'_>) -> Option<Option<Val<'a>>> {
    let m = ctx.message;
    Some(match field {
        FieldRef::Key => m.key.as_deref().map(Val::Str),
        FieldRef::Partition => Some(Val::Num(Cow::Owned(Number::from(m.partition)))),
        FieldRef::Header(name) => m
            .headers
            .get(name)
            .or_else(|| {
                m.headers
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(name))
                    .map(|(_, v)| v)
            })
            .map(|v| Val::Str(v)),
        FieldRef::Payload(path) => {
            let doc = ctx.doc()?;
            walk(doc, path).map(Val::of)
        }
    })
}

/// Dot path with array indices (`order.items.0.sku`). An empty path is the whole document.
fn walk<'v>(doc: &'v Value, path: &str) -> Option<&'v Value> {
    if path.is_empty() {
        return Some(doc);
    }
    let mut cur = doc;
    for seg in path.split('.') {
        cur = match cur {
            Value::Object(map) => map.get(seg)?,
            Value::Array(items) => items.get(seg.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cur)
}

fn eval(c: &Condition, ctx: &MatchContext<'_>) -> bool {
    let Some(field) = resolve(&c.field, ctx) else {
        return false;
    };
    match c.op {
        Op::Exists => return field.is_some(),
        Op::NotExists => return field.is_none(),
        _ => {}
    }
    let lit = match &c.value {
        Some(v) => Val::of(v),
        None => Val::Null,
    };
    let in_list = |f: &Val<'_>| match &lit {
        Val::Array(items) => items.iter().any(|i| val_eq(f, &Val::of(i))),
        single => val_eq(f, single),
    };
    match (c.op, field) {
        (Op::Ne, None) | (Op::NotIn, None) => true,
        (_, None) => false,
        (Op::Eq, Some(f)) => val_eq(&f, &lit),
        (Op::Ne, Some(f)) => !val_eq(&f, &lit),
        (Op::Gt, Some(f)) => val_cmp(&f, &lit) == Some(Ordering::Greater),
        (Op::Gte, Some(f)) => {
            matches!(val_cmp(&f, &lit), Some(Ordering::Greater | Ordering::Equal))
        }
        (Op::Lt, Some(f)) => val_cmp(&f, &lit) == Some(Ordering::Less),
        (Op::Lte, Some(f)) => {
            matches!(val_cmp(&f, &lit), Some(Ordering::Less | Ordering::Equal))
        }
        (Op::In, Some(f)) => in_list(&f),
        (Op::NotIn, Some(f)) => !in_list(&f),
        (Op::Prefix, Some(f)) => match (f.text(), lit.text()) {
            (Some(t), Some(p)) => t.starts_with(&*p),
            _ => false,
        },
        (Op::Contains, Some(Val::Array(items))) => items.iter().any(|i| val_eq(&Val::of(i), &lit)),
        (Op::Contains, Some(f)) => match (f.text(), lit.text()) {
            (Some(t), Some(p)) => t.contains(&*p),
            _ => false,
        },
        (Op::Exists | Op::NotExists, _) => unreachable!("handled above"),
    }
}

#[cfg(test)]
mod tests;
