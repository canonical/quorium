use quorium_core::hash::fnv1a;

#[test]
fn matches_fnv1a_reference_vectors() {
    assert_eq!(fnv1a(b""), 0xcbf2_9ce4_8422_2325);
    assert_eq!(fnv1a(b"a"), 0xaf63_dc4c_8601_ec8c);
    assert_eq!(fnv1a(b"foobar"), 0x85944171f73967e8);
}
