use std::collections::BTreeMap;

use bytes::Bytes;
use serde_json::json;

use super::*;

fn c(field: FieldRef, op: Op, value: Option<Value>) -> Condition {
    Condition { field, op, value }
}

fn p(path: &str) -> FieldRef {
    FieldRef::Payload(path.to_string())
}

fn msg(payload: &str) -> Message {
    Message {
        partition: 3,
        offset: 42,
        key: Some("user-7".into()),
        headers: BTreeMap::from([
            ("flux-type".to_string(), "order.created".to_string()),
            ("Priority".to_string(), "7".to_string()),
            ("flag".to_string(), "true".to_string()),
        ]),
        timestamp_ms: 1,
        payload: Bytes::from(payload.to_string()),
    }
}

fn m(filter: &str, payload: &str) -> bool {
    let conds = parse(filter).unwrap_or_else(|e| panic!("{filter}: {e}"));
    matches(&conds, &msg(payload))
}

fn err(filter: &str) -> FilterError {
    parse(filter).expect_err(filter)
}

// ---- parse --------------------------------------------------------------------------------

#[test]
fn parses_contract_example() {
    let got = parse(r#"type = "order.created" AND country = "MD" AND amount >= 100"#).unwrap();
    assert_eq!(
        got,
        vec![
            c(p("type"), Op::Eq, Some(json!("order.created"))),
            c(p("country"), Op::Eq, Some(json!("MD"))),
            c(p("amount"), Op::Gte, Some(json!(100))),
        ]
    );
}

#[test]
fn parses_all_operators() {
    let cases: &[(&str, Op, Option<Value>)] = &[
        ("a = 1", Op::Eq, Some(json!(1))),
        ("a == 1", Op::Eq, Some(json!(1))),
        ("a != 1", Op::Ne, Some(json!(1))),
        ("a <> 1", Op::Ne, Some(json!(1))),
        ("a > 1", Op::Gt, Some(json!(1))),
        ("a >= 1", Op::Gte, Some(json!(1))),
        ("a < 1", Op::Lt, Some(json!(1))),
        ("a <= 1", Op::Lte, Some(json!(1))),
        ("a in [1, 2]", Op::In, Some(json!([1, 2]))),
        ("a IN (1,2)", Op::In, Some(json!([1, 2]))),
        ("a not in [x]", Op::NotIn, Some(json!(["x"]))),
        ("a NOT IN [x]", Op::NotIn, Some(json!(["x"]))),
        ("a prefix ord", Op::Prefix, Some(json!("ord"))),
        ("a CONTAINS \"x\"", Op::Contains, Some(json!("x"))),
        ("a exists", Op::Exists, None),
        ("a Exists", Op::Exists, None),
        ("a not exists", Op::NotExists, None),
        ("a NOT EXISTS", Op::NotExists, None),
    ];
    for (input, op, value) in cases {
        assert_eq!(
            parse(input).unwrap(),
            vec![c(p("a"), *op, value.clone())],
            "{input}"
        );
    }
}

#[test]
fn parses_fields() {
    let f = |s: &str| parse(&format!("{s} exists")).unwrap().remove(0).field;
    assert_eq!(f("key"), FieldRef::Key);
    assert_eq!(f("KEY"), FieldRef::Key);
    assert_eq!(f("partition"), FieldRef::Partition);
    assert_eq!(f("header.flux-type"), FieldRef::Header("flux-type".into()));
    assert_eq!(f("HEADER.X-Id"), FieldRef::Header("X-Id".into()));
    assert_eq!(f("header.\"a b\""), FieldRef::Header("a b".into()));
    assert_eq!(f("payload.key"), p("key"));
    assert_eq!(f("payload.\"x y\""), p("x y"));
    assert_eq!(f("order.items.0.sku"), p("order.items.0.sku"));
    assert_eq!(f("\"weird path\""), p("weird path"));
    assert_eq!(f("0"), p("0"));
    assert_eq!(f("header"), p("header"));
    assert_eq!(f("in"), p("in"));
    assert_eq!(f("not"), p("not"));
    assert_eq!(f("цена"), p("цена"));
}

#[test]
fn parses_values() {
    let v = |s: &str| parse(&format!("a = {s}")).unwrap().remove(0).value.unwrap();
    assert_eq!(v("8842"), json!(8842));
    assert_eq!(v("-5"), json!(-5));
    assert_eq!(v("-12.5"), json!(-12.5));
    assert_eq!(v("1e3"), json!(1000.0));
    assert_eq!(v("2.5E-1"), json!(0.25));
    assert_eq!(v("18446744073709551615"), json!(u64::MAX));
    assert_eq!(v("true"), json!(true));
    assert_eq!(v("FALSE"), json!(false));
    assert_eq!(v("null"), Value::Null);
    assert_eq!(v("MD"), json!("MD"));
    assert_eq!(v("order.created"), json!("order.created"));
    assert_eq!(v("2024-01-01"), json!("2024-01-01"));
    assert_eq!(v("007"), json!("007"));
    assert_eq!(v("'single'"), json!("single"));
    assert_eq!(
        v(r#""q\"uote\\ \n\t\u0041\ud83d\ude00""#),
        json!("q\"uote\\ \n\tA😀")
    );
    assert_eq!(v(r#""8842""#), json!("8842"));
}

#[test]
fn empty_and_whitespace_filters_match_everything() {
    assert_eq!(parse("").unwrap(), vec![]);
    assert_eq!(parse("  \n\t ").unwrap(), vec![]);
}

#[test]
fn and_is_case_insensitive() {
    assert_eq!(parse("a=1 and b=2 AnD c=3").unwrap().len(), 3);
}

#[test]
fn errors_have_precise_positions() {
    let cases: &[(&str, usize, &str)] = &[
        ("a = ", 4, "expected a value"),
        ("a", 1, "expected an operator"),
        ("a ~ 1", 2, "unexpected character '~'"),
        ("a = \"open", 4, "unterminated string"),
        ("a = 1 b = 2", 6, "expected AND"),
        ("a = 1 OR b = 2", 6, "OR is not supported"),
        ("a = 1 AND", 9, "expected a condition after AND"),
        ("a in 1", 5, "expected '['"),
        ("a in [1, 2", 10, "expected ',' or ']'"),
        ("a in [1 2]", 8, "expected ',' or ']'"),
        ("a = [1]", 4, "lists are only allowed"),
        ("a not 1", 6, "expected IN or EXISTS"),
        ("a ! 1", 2, "did you mean '!='"),
        ("= 1", 0, "expected a field"),
        ("AND a = 1", 0, "expected a field"),
        ("a = AND", 4, "expected a value"),
        ("a = \"\\x\"", 5, "invalid escape"),
        ("a = \"\\u12\"", 5, "invalid \\u escape"),
        ("header. = 1", 7, "expected header name"),
        ("a in [[1]]", 6, "lists are only allowed"),
    ];
    for (input, pos, msg) in cases {
        let e = err(input);
        assert_eq!(e.position, *pos, "{input}: {e}");
        assert!(e.message.contains(msg), "{input}: {e}");
    }
}

#[test]
fn error_display() {
    assert_eq!(
        err("a ~").to_string(),
        "filter syntax error at 2: unexpected character '~'"
    );
}

#[test]
fn error_positions_are_byte_offsets_with_unicode() {
    let e = err("цена ~ 1");
    assert_eq!(e.position, "цена ".len());
}

// ---- render -------------------------------------------------------------------------------

#[test]
fn renders_canonically() {
    let conds = parse(
        r#"type == 'order.created' and header.X-Id exists AND amount>=100.5 and tags not in (a, "b c")
           AND payload.key = 1 AND key prefix "user-" AND "a b" != null AND partition < 3"#,
    )
    .unwrap();
    assert_eq!(
        render(&conds),
        r#"type = "order.created" AND header.X-Id exists AND amount >= 100.5 AND tags not in ["a", "b c"] AND payload.key = 1 AND key prefix "user-" AND payload."a b" != null AND partition < 3"#
    );
}

#[test]
fn renders_reserved_payload_paths_with_prefix() {
    for path in [
        "key",
        "partition",
        "header.x",
        "payload.y",
        "and",
        "OR",
        "Key",
    ] {
        let conds = vec![c(p(path), Op::Exists, None)];
        let text = render(&conds);
        assert!(text.starts_with("payload."), "{text}");
        assert_eq!(parse(&text).unwrap(), conds, "{text}");
    }
}

#[test]
fn render_parse_roundtrip_examples() {
    let conds = vec![
        c(FieldRef::Header("".into()), Op::Exists, None),
        c(p(""), Op::NotExists, None),
        c(p("x"), Op::Eq, Some(json!("line\nbreak \u{1} \"q\" \\"))),
        c(p("n"), Op::Eq, Some(json!(-0.0))),
        c(p("n"), Op::Gt, Some(json!(1e300))),
        c(p("n"), Op::Lt, Some(json!(i64::MIN))),
        c(p("1e5"), Op::Eq, Some(json!(5.0))),
        c(p("+x"), Op::Eq, Some(json!(true))),
        c(p("a"), Op::In, Some(json!([null, false, "s", 1.5]))),
    ];
    let text = render(&conds);
    assert_eq!(parse(&text).unwrap(), conds, "{text}");
}

// ---- matches ------------------------------------------------------------------------------

const ORDER: &str = r#"{"type":"order.created","order_id":"8842","amount":120.5,"qty":3,
  "country":"MD","vip":true,"note":null,"tags":["new","vip"],
  "order":{"items":[{"sku":"A-1","price":10},{"sku":"B-2","price":99}]}}"#;

#[test]
fn payload_equality_and_numeric_string() {
    assert!(m("type = order.created", ORDER));
    assert!(m("order_id = 8842", ORDER));
    assert!(m("order_id = \"8842\"", ORDER));
    assert!(!m("order_id = 8843", ORDER));
    assert!(m("qty = \"3\"", ORDER));
    assert!(m("qty = 3.0", ORDER));
    assert!(m("amount = 120.5", ORDER));
    assert!(m("vip = true", ORDER));
    assert!(!m("vip = \"yes\"", ORDER));
    assert!(m("note = null", ORDER));
    assert!(!m("country = md", ORDER));
}

#[test]
fn comparisons() {
    assert!(m("amount >= 100", ORDER));
    assert!(m("amount > 120", ORDER));
    assert!(!m("amount > 120.5", ORDER));
    assert!(m("amount <= 120.5", ORDER));
    assert!(m("amount < 121", ORDER));
    assert!(m("order_id > 8000", ORDER), "numeric string vs number");
    assert!(m("country > MA", ORDER), "lexical");
    assert!(!m("country < MA", ORDER));
    assert!(!m("vip > 0", ORDER), "bool is not comparable");
    assert!(!m("type > 5", ORDER), "non-numeric string vs number");
    assert!(m("header.Priority > 5", ORDER));
    assert!(m("partition >= 3 AND partition < 4", ORDER));
}

#[test]
fn big_integers_compare_exactly() {
    let doc = r#"{"a": 9007199254740993, "b": 18446744073709551615, "c": -1}"#;
    assert!(m("a = 9007199254740993", doc));
    assert!(!m("a = 9007199254740992", doc));
    assert!(m("b > 9223372036854775807", doc));
    assert!(m("c < 18446744073709551615", doc));
    assert!(m("b > -1", doc));
}

#[test]
fn lists_prefix_contains() {
    assert!(m("country in [MD, RO]", ORDER));
    assert!(!m("country in [RO, UA]", ORDER));
    assert!(m("country not in [RO, UA]", ORDER));
    assert!(m("order_id in [1, 8842]", ORDER));
    assert!(m("type prefix order.", ORDER));
    assert!(!m("type prefix created", ORDER));
    assert!(m("type contains created", ORDER));
    assert!(m("tags contains vip", ORDER));
    assert!(!m("tags contains old", ORDER));
    assert!(m("qty prefix 3", ORDER));
    assert!(m("key prefix user-", ORDER));
    assert!(m("key contains \"-7\"", ORDER));
}

#[test]
fn paths_with_array_indices() {
    assert!(m("order.items.1.sku = B-2", ORDER));
    assert!(m("order.items.0.price < 20", ORDER));
    assert!(m("tags.0 = new", ORDER));
    assert!(m("order.items.2 not exists", ORDER));
    assert!(m("order.items.x not exists", ORDER));
    assert!(m("type.deeper not exists", ORDER));
}

#[test]
fn exists_semantics() {
    assert!(m("note exists", ORDER), "null counts as present");
    assert!(m("missing not exists", ORDER));
    assert!(!m("missing exists", ORDER));
    assert!(m("missing != 1", ORDER), "!= matches an absent field");
    assert!(m("missing not in [1]", ORDER));
    assert!(!m("missing = 1", ORDER));
    assert!(!m("missing > 1", ORDER));
    assert!(!m("missing prefix a", ORDER));
}

#[test]
fn headers_key_partition() {
    assert!(m("header.flux-type = order.created", "x"));
    assert!(m("header.priority = 7", "x"), "case-insensitive fallback");
    assert!(m("header.flag = true", "x"));
    assert!(m("header.nope not exists", "x"));
    assert!(m("key = user-7", "x"));
    assert!(m("partition = 3", "x"));
    assert!(m("partition = \"3\"", "x"));
    assert!(!m("partition = 4", "x"));
    let mut no_key = msg("x");
    no_key.key = None;
    assert!(matches(&parse("key not exists").unwrap(), &no_key));
    assert!(!matches(&parse("key = x").unwrap(), &no_key));
}

#[test]
fn non_json_payload_never_matches_payload_conditions() {
    for f in [
        "a = 1",
        "a != 1",
        "a not exists",
        "a exists",
        "a not in [1]",
    ] {
        assert!(!m(f, "not json {"), "{f}");
    }
    assert!(m("key = user-7", "not json {"));
}

#[test]
fn empty_conditions_match_everything() {
    assert!(matches(&[], &msg("whatever")));
}

#[test]
fn payload_is_not_parsed_when_cheap_conditions_fail() {
    let conds = parse("a = 1 AND key = nope").unwrap();
    let message = msg(r#"{"a":1}"#);
    let ctx = MatchContext::new(&message);
    assert!(!matches_in(&conds, &ctx));
    assert!(ctx.doc.get().is_none(), "payload must not be parsed");
    let conds = parse("a = 1 AND key = user-7").unwrap();
    assert!(matches_in(&conds, &ctx));
    assert!(ctx.doc.get().is_some());
}

#[test]
fn structured_values_from_api_are_handled() {
    // The structured editor may send a scalar for `in` — treated as a one-element list.
    let conds = vec![c(p("country"), Op::In, Some(json!("MD")))];
    assert!(matches(&conds, &msg(ORDER)));
    assert_eq!(render(&conds), "country in [\"MD\"]");
    // Object equality.
    let conds = vec![c(
        p("order.items.0"),
        Op::Eq,
        Some(json!({"sku":"A-1","price":10})),
    )];
    assert!(matches(&conds, &msg(ORDER)));
}

// ---- property / fuzz ----------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64*
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len() as u64) as usize]
    }
}

const ATOMS: &[&str] = &[
    "a",
    "key",
    "partition",
    "header",
    "payload",
    "and",
    "OR",
    "in",
    "not",
    "exists",
    "x.y",
    "0",
    "-1",
    "1e5",
    " ",
    ".",
    "\"",
    "\\",
    "'",
    "é",
    "日本",
    "\n",
    "=",
    "[",
    "-",
    "+",
    "_",
    "true",
    "null",
    "Key",
];

fn rand_text(r: &mut Rng) -> String {
    (0..r.below(4)).map(|_| *r.pick(ATOMS)).collect()
}

fn rand_scalar(r: &mut Rng) -> Value {
    match r.below(8) {
        0 => Value::Null,
        1 => json!(r.below(2) == 0),
        2 => json!(r.next() as i64),
        3 => json!(r.next()),
        4 => json!((r.next() as i64 as f64) / 1000.0),
        5 => json!(f64::from_bits(
            r.next() & !(0x7ff << 52) | (r.below(2000) << 52)
        )),
        _ => json!(rand_text(r)),
    }
}

fn rand_condition(r: &mut Rng) -> Condition {
    let field = match r.below(4) {
        0 => FieldRef::Key,
        1 => FieldRef::Partition,
        2 => FieldRef::Header(rand_text(r)),
        _ => FieldRef::Payload(rand_text(r)),
    };
    let op = *r.pick(&[
        Op::Eq,
        Op::Ne,
        Op::Gt,
        Op::Gte,
        Op::Lt,
        Op::Lte,
        Op::In,
        Op::NotIn,
        Op::Prefix,
        Op::Contains,
        Op::Exists,
        Op::NotExists,
    ]);
    let value = match op {
        Op::Exists | Op::NotExists => None,
        Op::In | Op::NotIn => Some(Value::Array(
            (0..1 + r.below(4)).map(|_| rand_scalar(r)).collect(),
        )),
        _ => Some(rand_scalar(r)),
    };
    Condition { field, op, value }
}

#[test]
fn prop_parse_render_roundtrip() {
    let mut r = Rng(0x9E37_79B9_7F4A_7C15);
    for _ in 0..20_000 {
        let conds: Vec<Condition> = (0..r.below(5)).map(|_| rand_condition(&mut r)).collect();
        let text = render(&conds);
        let back = parse(&text).unwrap_or_else(|e| panic!("{text:?}: {e}"));
        assert_eq!(back, conds, "{text}");
        assert_eq!(render(&back), text);
    }
}

const FUZZ_PIECES: &[&str] = &[
    "a",
    "b.c",
    " ",
    "=",
    "!=",
    ">",
    ">=",
    "<",
    "<=",
    "==",
    "<>",
    "in",
    "not",
    "exists",
    "prefix",
    "contains",
    "AND",
    "and",
    "or",
    "[",
    "]",
    "(",
    ")",
    ",",
    "\"s\"",
    "'t'",
    "\"",
    "\\",
    "1",
    "-2.5",
    "1e3",
    "true",
    "null",
    "key",
    "partition",
    "header.",
    "header.x",
    "payload.",
    "!",
    "~",
    "é",
    "\"\\u0041\"",
    "\"\\ud800\"",
];

const FIELDS: &[&str] = &[
    "a",
    "b.c.0",
    "key",
    "partition",
    "header.x",
    "header.\"a b\"",
    "payload.key",
    "\"q r\"",
    "0",
    "in",
    "日本",
];
const OPS: &[&str] = &[
    "=",
    "!=",
    ">",
    ">=",
    "<",
    "<=",
    "==",
    "<>",
    "prefix",
    "contains",
    "exists",
    "not exists",
    "in [1, x]",
    "not in (\"s\", null)",
    "IN [true]",
];
const VALUES: &[&str] = &[
    "1",
    "-2.5",
    "1e3",
    "true",
    "null",
    "MD",
    "\"8842\"",
    "'it\\'s'",
    "x.y",
    "\"\\u00e9\"",
    "",
];

#[test]
fn prop_render_parse_idempotent_and_no_panics() {
    let mut r = Rng(0xDEAD_BEEF_CAFE_F00D);
    let mut parsed = 0;
    for _ in 0..50_000 {
        // Mostly grammar-shaped input, randomly mutated, plus some pure noise.
        let mut pieces: Vec<&str> = Vec::new();
        if r.below(4) == 0 {
            for _ in 0..1 + r.below(12) {
                pieces.push(*r.pick(FUZZ_PIECES));
            }
        } else {
            for i in 0..1 + r.below(3) {
                if i > 0 {
                    pieces.push(*r.pick(&["AND", "and", "AnD"]));
                }
                pieces.push(*r.pick(FIELDS));
                pieces.push(*r.pick(OPS));
                pieces.push(*r.pick(VALUES));
            }
            for _ in 0..r.below(2) {
                let at = r.below(pieces.len() as u64 + 1) as usize;
                if r.below(2) == 0 && at < pieces.len() {
                    pieces.remove(at);
                } else {
                    pieces.insert(at, *r.pick(FUZZ_PIECES));
                }
            }
        }
        let text = pieces.join(if r.below(5) == 0 { "" } else { " " });
        match parse(&text) {
            Ok(conds) => {
                parsed += 1;
                let canon = render(&conds);
                let again = parse(&canon).unwrap_or_else(|e| panic!("{text:?} → {canon:?}: {e}"));
                assert_eq!(again, conds, "{text:?} → {canon:?}");
                assert_eq!(render(&again), canon);
                // Evaluation never panics either.
                let _ = matches(&conds, &msg(ORDER));
            }
            Err(e) => assert!(e.position <= text.len(), "{text:?}: {e}"),
        }
    }
    assert!(parsed > 1000, "fuzzer too weak: {parsed}");
}

#[test]
fn prop_random_bytes_never_panic() {
    let mut r = Rng(42);
    for _ in 0..20_000 {
        let bytes: Vec<u8> = (0..r.below(24)).map(|_| r.below(128) as u8).collect();
        let text = String::from_utf8_lossy(&bytes);
        if let Err(e) = parse(&text) {
            assert!(e.position <= text.len());
            assert!(text.is_char_boundary(e.position));
        }
    }
}
