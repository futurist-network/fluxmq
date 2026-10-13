//! RFC 7386 JSON merge patch: diff and apply.
//!
//! A merge patch cannot express "set this key to `null`" (null means delete). To keep
//! `apply(diff(a, b), a) == b` exact, every document the hub publishes is first passed through
//! [`strip_nulls`], which drops `null` object members recursively. Rust deserializers treat a
//! missing `Option` field as `None`, so the stripped form is equivalent for every consumer.

use serde_json::{Map, Value};

/// Removes `null`-valued object members recursively (array elements are kept as-is, but
/// objects inside arrays are stripped too).
pub fn strip_nulls(value: &mut Value) {
    match value {
        Value::Object(map) => {
            map.retain(|_, v| !v.is_null());
            for v in map.values_mut() {
                strip_nulls(v);
            }
        }
        Value::Array(items) => {
            for v in items {
                strip_nulls(v);
            }
        }
        _ => {}
    }
}

/// Computes the merge patch that turns `old` into `new`. Returns `None` when they are equal
/// (no patch needs to be sent).
///
/// * both objects → recurse per key; keys missing from `new` become `null`;
/// * anything else (arrays, scalars, type changes) → `new` replaces `old` wholesale.
///
/// `new` must not contain `null` object members (see [`strip_nulls`]); otherwise the patch would
/// delete those keys instead of setting them to `null`.
pub fn diff(old: &Value, new: &Value) -> Option<Value> {
    match (old, new) {
        (Value::Object(o), Value::Object(n)) => {
            let mut patch = Map::new();
            for (k, ov) in o {
                match n.get(k) {
                    None => {
                        patch.insert(k.clone(), Value::Null);
                    }
                    Some(nv) => {
                        if let Some(p) = diff(ov, nv) {
                            patch.insert(k.clone(), p);
                        }
                    }
                }
            }
            for (k, nv) in n {
                if !o.contains_key(k) {
                    patch.insert(k.clone(), nv.clone());
                }
            }
            if patch.is_empty() {
                None
            } else {
                Some(Value::Object(patch))
            }
        }
        _ if old == new => None,
        // Replacing an object with a non-object (or vice versa) is a wholesale replacement.
        // A non-object patch replaces the target entirely per RFC 7386. An object `new`
        // replacing a non-object `old` is applied onto `{}`, which yields `new` exactly
        // because `new` contains no nulls.
        _ => Some(new.clone()),
    }
}

/// Applies an RFC 7386 merge patch to `target` in place.
pub fn apply(target: &mut Value, patch: &Value) {
    match patch {
        Value::Object(p) => {
            if !target.is_object() {
                *target = Value::Object(Map::new());
            }
            if let Value::Object(t) = target {
                for (k, pv) in p {
                    if pv.is_null() {
                        t.remove(k);
                    } else {
                        let entry = t.entry(k.clone()).or_insert(Value::Null);
                        apply(entry, pv);
                    }
                }
            }
        }
        other => *target = other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn equal_documents_produce_no_patch() {
        let a = json!({"a": 1, "b": {"c": [1, 2]}});
        assert_eq!(diff(&a, &a.clone()), None);
    }

    #[test]
    fn scalar_change_and_nested_objects() {
        let a = json!({"rate": 1.5, "lat": {"p50": 1.0, "p99": 2.0}, "name": "x"});
        let b = json!({"rate": 2.5, "lat": {"p50": 1.0, "p99": 3.0}, "name": "x"});
        assert_eq!(
            diff(&a, &b),
            Some(json!({"rate": 2.5, "lat": {"p99": 3.0}}))
        );
    }

    #[test]
    fn removed_keys_become_null_and_added_keys_appear() {
        let a = json!({"groups": {"g1": {"lag": 1}, "g2": {"lag": 2}}});
        let b = json!({"groups": {"g1": {"lag": 1}, "g3": {"lag": 3}}});
        assert_eq!(
            diff(&a, &b),
            Some(json!({"groups": {"g2": null, "g3": {"lag": 3}}}))
        );
    }

    #[test]
    fn arrays_replace_wholesale() {
        let a = json!({"p": [1, 2, 3]});
        let b = json!({"p": [1, 2, 4]});
        assert_eq!(diff(&a, &b), Some(json!({"p": [1, 2, 4]})));
    }

    #[test]
    fn type_changes_replace() {
        assert_eq!(
            diff(&json!({"a": {"b": 1}}), &json!({"a": 5})),
            Some(json!({"a": 5}))
        );
        assert_eq!(
            diff(&json!({"a": 5}), &json!({"a": {"b": 1}})),
            Some(json!({"a": {"b": 1}}))
        );
        assert_eq!(diff(&json!(1), &json!({"x": 1})), Some(json!({"x": 1})));
        assert_eq!(diff(&json!({"x": 1}), &json!([1])), Some(json!([1])));
    }

    #[test]
    fn strip_nulls_recurses() {
        let mut v = json!({"a": null, "b": {"c": null, "d": 1}, "e": [{"f": null}, null]});
        strip_nulls(&mut v);
        assert_eq!(v, json!({"b": {"d": 1}, "e": [{}, null]}));
    }

    #[test]
    fn apply_rfc7386_examples() {
        // Examples from RFC 7386 appendix A.
        let cases = [
            (json!({"a": "b"}), json!({"a": "c"}), json!({"a": "c"})),
            (
                json!({"a": "b"}),
                json!({"b": "c"}),
                json!({"a": "b", "b": "c"}),
            ),
            (json!({"a": "b"}), json!({"a": null}), json!({})),
            (
                json!({"a": "b", "b": "c"}),
                json!({"a": null}),
                json!({"b": "c"}),
            ),
            (json!({"a": ["b"]}), json!({"a": "c"}), json!({"a": "c"})),
            (json!({"a": "c"}), json!({"a": ["b"]}), json!({"a": ["b"]})),
            (
                json!({"a": {"b": "c"}}),
                json!({"a": {"b": "d", "c": null}}),
                json!({"a": {"b": "d"}}),
            ),
            (
                json!({"a": [{"b": "c"}]}),
                json!({"a": [1]}),
                json!({"a": [1]}),
            ),
            (json!(["a", "b"]), json!(["c", "d"]), json!(["c", "d"])),
            (json!({"a": "b"}), json!(["c"]), json!(["c"])),
            (
                json!({"e": null}),
                json!({"a": 1}),
                json!({"e": null, "a": 1}),
            ),
            (
                json!([1, 2]),
                json!({"a": "b", "c": null}),
                json!({"a": "b"}),
            ),
            (
                json!({}),
                json!({"a": {"bb": {"ccc": null}}}),
                json!({"a": {"bb": {}}}),
            ),
        ];
        for (mut target, patch, expected) in cases {
            apply(&mut target, &patch);
            assert_eq!(target, expected, "patch {patch}");
        }
    }

    /// Tiny deterministic PRNG (xorshift) so the property test needs no extra deps.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    fn random_value(rng: &mut Rng, depth: u32) -> Value {
        let choice = if depth == 0 {
            rng.below(5)
        } else {
            rng.below(7)
        };
        match choice {
            0 => Value::Null,
            1 => Value::Bool(rng.below(2) == 0),
            2 => json!(rng.below(5)),
            3 => json!(rng.below(4) as f64 / 2.0),
            4 => json!(["s", "t", "u"][rng.below(3) as usize]),
            5 => Value::Array(
                (0..rng.below(3))
                    .map(|_| random_value(rng, depth - 1))
                    .collect(),
            ),
            _ => {
                let mut m = Map::new();
                for _ in 0..rng.below(5) {
                    let key = ["a", "b", "c", "d", "e"][rng.below(5) as usize].to_string();
                    m.insert(key, random_value(rng, depth - 1));
                }
                Value::Object(m)
            }
        }
    }

    #[test]
    fn property_apply_diff_roundtrips() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        for _ in 0..5_000 {
            let mut a = random_value(&mut rng, 4);
            let mut b = random_value(&mut rng, 4);
            strip_nulls(&mut a);
            strip_nulls(&mut b);
            match diff(&a, &b) {
                None => assert_eq!(a, b),
                Some(p) => {
                    if a.is_object() {
                        assert_ne!(p, json!({}), "empty patches must be skipped");
                    }
                    let mut t = a.clone();
                    apply(&mut t, &p);
                    assert_eq!(t, b, "a={a} b={b} patch={p}");
                }
            }
        }
    }
}
