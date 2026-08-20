//! Replica identity and TOAST: substitution under FULL, typed refusal
//! without it, the per-table preflight matrix, and declared-key overrides.

use rdlt_engine::config::Config as EngineConfig;
use rdlt_engine::engine::Engine;

use crate::cases::cdc_rig::Rig;
use crate::cases::common::source;

// ─────────────── TOAST policy + error matrix + lag ───────────────

#[tokio::test(flavor = "multi_thread")]
async fn toast_full_identity_substitutes_from_the_old_image() {
    // Retain semantics: an unchanged out-of-line value rides through an
    // unrelated update because REPLICA IDENTITY FULL carries the old image.
    let Some(rig) = Rig::start("cdc-toast-full").await else {
        return;
    };
    rig.container
        .seed(
            "CREATE TABLE public.docs (id int8 PRIMARY KEY, blob text, counter int4);\
             ALTER TABLE public.docs REPLICA IDENTITY FULL;\
             INSERT INTO public.docs \
             SELECT 1, (SELECT string_agg(md5(g::text), '') FROM generate_series(1, 4000) g), 1;",
        )
        .await;

    rig.run(&["docs"], "id").await;
    rig.assert_mirror_equals_source("docs", "id, blob, counter")
        .await;

    // Unrelated update: blob untouched (arrives as an unchanged-TOAST marker).
    rig.container
        .seed("UPDATE public.docs SET counter = 2 WHERE id = 1;")
        .await;
    rig.run(&["docs"], "id").await;
    assert_eq!(
        rig.scalar("SELECT counter::int8 FROM mirror.docs WHERE id = 1")
            .await,
        2
    );
    assert_eq!(
        rig.scalar(
            "SELECT count(*) FROM mirror.docs m JOIN public.docs p USING (id) \
             WHERE m.blob = p.blob AND length(m.blob) > 100000"
        )
        .await,
        1,
        "the TOAST value survived the unrelated update intact"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn toast_without_full_identity_fails_typed_never_nulls() {
    // The other half: same shape under DEFAULT identity — no old image to
    // substitute from; typed error naming table + column + the ALTER.
    let Some(rig) = Rig::start("cdc-toast-default").await else {
        return;
    };
    rig.container
        .seed(
            "CREATE TABLE public.docs (id int8 PRIMARY KEY, blob text, counter int4);\
             INSERT INTO public.docs \
             SELECT 1, (SELECT string_agg(md5(g::text), '') FROM generate_series(1, 4000) g), 1;",
        )
        .await;
    rig.run(&["docs"], "id").await;
    rig.container
        .seed("UPDATE public.docs SET counter = 2 WHERE id = 1;")
        .await;
    let error = rig.run_expecting_error(&["docs"], "id").await;
    assert!(error.contains("`blob`"), "{error}");
    assert!(error.contains("REPLICA IDENTITY FULL"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn identity_preflight_matrix_is_typed_per_table() {
    let Some(rig) = Rig::start("cdc-identity").await else {
        return;
    };
    rig.container
        .seed(
            "CREATE TABLE public.nopk (v int4);\
             CREATE TABLE public.nothing (id int8 PRIMARY KEY, v int4);\
             ALTER TABLE public.nothing REPLICA IDENTITY NOTHING;\
             CREATE TABLE public.collides (id int8 PRIMARY KEY, _rdlt_deleted bool);",
        )
        .await;

    // PK-less default identity: named table + the fix.
    let error = rig.run_expecting_error(&["nopk"], "v").await;
    assert!(error.contains("`nopk`"), "{error}");
    assert!(error.contains("REPLICA IDENTITY"), "{error}");

    // REPLICA IDENTITY NOTHING: unusable even with a PK.
    let error = rig.run_expecting_error(&["nothing"], "id").await;
    assert!(error.contains("`nothing`"), "{error}");
    assert!(error.contains("replica identity"), "{error}");

    // Flag-column collision: named table + column.
    let error = rig.run_expecting_error(&["collides"], "id").await;
    assert!(error.contains("`collides`"), "{error}");
    assert!(error.contains("_rdlt_deleted"), "{error}");

    // cdc + cursor exclusivity is a CONFIG-parse error — no server
    // round-trip, named table.
    let error = rdlt_connector_postgres::source::Shell::from_yaml(
        "conn: host=localhost\ncdc:\n  slot: s\n  publication: p\n\
         tables:\n  - name: t\n    cursor:\n      column: id\n",
    )
    .expect_err("exclusivity")
    .to_string();
    assert!(
        error.contains("`t`") && error.contains("mutually exclusive"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn identity_dropped_mid_stream_never_misapplies() {
    // The identity weakens AFTER the pipeline is established — the next run
    // refuses at preflight, before any change could be mis-applied.
    let Some(rig) = Rig::start("cdc-identity-drop").await else {
        return;
    };
    rig.container
        .seed(
            "CREATE TABLE public.ev (id int8, v int4, CONSTRAINT ev_pk PRIMARY KEY (id));\
             INSERT INTO public.ev VALUES (1, 1);",
        )
        .await;
    rig.run(&["ev"], "id").await;

    rig.container
        .seed("ALTER TABLE public.ev DROP CONSTRAINT ev_pk; INSERT INTO public.ev VALUES (2, 2);")
        .await;
    let error = rig.run_expecting_error(&["ev"], "id").await;
    assert!(error.contains("`ev`"), "{error}");
    assert!(error.contains("REPLICA IDENTITY"), "{error}");
    // Nothing moved: the mirror still holds exactly the pre-drop state.
    assert_eq!(rig.scalar("SELECT count(*) FROM mirror.ev").await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn declared_primary_key_override_keys_the_stream_under_full() {
    // Under REPLICA IDENTITY FULL a declared primary_key override must win
    // over the catalog PK (any key has values in the full old image) — not
    // be silently ignored.
    use rdlt_connector_sdk::spi::source::Source;
    let Some(rig) = Rig::start("cdc-key-override").await else {
        return;
    };
    rig.container
        .seed(
            "CREATE TABLE public.orders (id int8 PRIMARY KEY, code text NOT NULL);\
             ALTER TABLE public.orders REPLICA IDENTITY FULL;\
             INSERT INTO public.orders VALUES (1, 'a');",
        )
        .await;
    let keyed_source = source(
        &rig.container.connection_string,
        "cdc:\n  slot: s1\n  publication: p1\n  create_if_missing: true\n\
         tables:\n  - name: orders\n    primary_key: [code]\n",
    );
    let specs = keyed_source.streams().await.expect("streams");
    assert_eq!(
        specs[0].primary_key.as_deref(),
        Some(&["code".to_string()][..]),
        "the declared business key wins under FULL"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn declared_key_mismatch_under_default_identity_is_typed() {
    // `primary_key` override × CDC: under DEFAULT replica identity the
    // delete records only carry the identity columns — a mismatching
    // override is a typed error, never silent mis-keying.
    let Some(rig) = Rig::start("cdc-key-mismatch").await else {
        return;
    };
    rig.container
        .seed(
            "CREATE TABLE public.orders (id int8 PRIMARY KEY, code text NOT NULL);\
             INSERT INTO public.orders VALUES (1, 'a');",
        )
        .await;
    let keyed_source = source(
        &rig.container.connection_string,
        "cdc:\n  slot: s1\n  publication: p1\n  create_if_missing: true\n\
         tables:\n  - name: orders\n    primary_key: [code]\n",
    );
    let config = EngineConfig::new("cdc-key-mismatch")
        .with_workdir(rig.workdir.clone())
        .with_write_mode(rdlt_connector_sdk::spi::core::commit::WriteMode::Merge {
            key: vec!["code".into()],
        });
    let error = Engine::new(config, keyed_source, rig.destination(&["orders"]))
        .run()
        .await
        .expect_err("mismatching override must fail typed")
        .to_string();
    assert!(
        error.contains("differs from the replica identity"),
        "{error}"
    );
    assert!(error.contains("`orders`"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn dropped_identity_index_is_typed_not_an_empty_key() {
    // relreplident stays 'i' after the identity index is dropped; the empty
    // column set must be a typed error, never an empty merge key.
    let Some(rig) = Rig::start("cdc-ident-index").await else {
        return;
    };
    rig.container
        .seed(
            "CREATE TABLE public.ev (id int8 NOT NULL, v int4);\
             CREATE UNIQUE INDEX ev_ident ON public.ev (id);\
             ALTER TABLE public.ev ALTER COLUMN id SET NOT NULL;\
             ALTER TABLE public.ev REPLICA IDENTITY USING INDEX ev_ident;\
             DROP INDEX public.ev_ident;\
             INSERT INTO public.ev VALUES (1, 1);",
        )
        .await;
    let error = rig.run_expecting_error(&["ev"], "id").await;
    assert!(error.contains("`ev`"), "{error}");
    assert!(error.contains("index"), "{error}");
}
