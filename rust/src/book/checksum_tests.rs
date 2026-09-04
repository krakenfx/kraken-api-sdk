use super::*;

/// Naive strip baseline — chunked `update_stripped` must byte-match.
fn strip_via_hasher(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len());
    let mut leading = true;
    for b in s.bytes() {
        if b == b'.' {
            continue;
        }
        if leading && b == b'0' {
            continue;
        }
        leading = false;
        out.push(b);
    }
    out
}

#[test]
fn strip_removes_decimal_and_leading_zeros() {
    assert_eq!(&strip_via_hasher("45285.21000000")[..], b"4528521000000");
    assert_eq!(&strip_via_hasher("0.00159953")[..], b"159953");
    assert_eq!(&strip_via_hasher("0")[..], b"");
    assert_eq!(&strip_via_hasher("0.0")[..], b"");
    assert_eq!(&strip_via_hasher("12345")[..], b"12345");
    assert_eq!(&strip_via_hasher("0.000123")[..], b"123");
}

/// Kraken's 281-char worked example MUST hash to `3310070434` (IEEE CRC-32).
#[test]
fn amendment_59_worked_example_matches_canon() {
    let input = "45285210000045286415457195345286615457110945289615456091145290215890660452918154553491452947445474945296135380000452975994554245299518772827452835100000004528341545820154528211000000045281010000000452803154592586452790799000045277633101034527753000000045277315460273745276615445238";
    assert_eq!(input.len(), 281, "amendment 59 input length");
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(input.as_bytes());
    let crc = hasher.finalize();
    assert_eq!(crc, 3_310_070_434, "amendment 59 expected CRC32");
}

/// Stripped forms longer than `STRIP_BUF_LEN` must match the naive single-shot strip.
#[test]
fn over_buffer_strings_hash_as_naive_strip() {
    let cases: &[(&str, &str)] = &[
        ("0.001234567890123456789012345678901234567890123", "0.5"),
        ("123456789012345678901234567.8901234567", "1"),
        ("12345678901234567890123456789012", "1"),
        ("123456789012345678901234567890123", "1"),
        (
            "9876543210987654321098765432109876543210987654321098765432109876543210",
            "0.000042",
        ),
    ];
    for (price, qty) in cases {
        let mut baseline = crc32fast::Hasher::new();
        baseline.update(&strip_via_hasher(price));
        baseline.update(&strip_via_hasher(qty));
        let expected = baseline.finalize();
        let got = compute_book_crc32([(*price, *qty)], []);
        assert_eq!(got, expected, "chunked hash diverged for {price:?}/{qty:?}");
    }
}

/// Strip + iterator + hasher under realistic wire-string lengths.
#[test]
fn compute_book_crc32_matches_hand_computed_baseline() {
    let asks: &[WireLevel<'static>] = &[("45285.21000000", "1.5"), ("45286.41545719", "53")];
    let bids: &[WireLevel<'static>] = &[("45282.11000000", "0.45"), ("45281.01000000", "0.0001")];
    let mut baseline = crc32fast::Hasher::new();
    baseline.update(b"4528521000000");
    baseline.update(b"15");
    baseline.update(b"4528641545719");
    baseline.update(b"53");
    baseline.update(b"4528211000000");
    baseline.update(b"45");
    baseline.update(b"4528101000000");
    baseline.update(b"1");
    let expected = baseline.finalize();

    let got = compute_book_crc32(asks.iter().copied(), bids.iter().copied());
    assert_eq!(got, expected);
}
