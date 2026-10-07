use super::*;
use crate::{SqlCipherKey, SqliteAccountStorage};
use rusqlite::{params, types::ValueRef};
use serde_json::{Value, json};
use std::error::Error;
type Result<T> = std::result::Result<T, Box<dyn Error>>;
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn typed(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => json!({"type": "null"}),
        ValueRef::Integer(value) => json!({"type": "integer", "value": value}),
        ValueRef::Real(value) => {
            json!({"type": "real", "bits": format!("{:016x}", value.to_bits())})
        }
        ValueRef::Text(value) => json!({"type": "text", "bytes_hex": hex(value)}),
        ValueRef::Blob(value) => json!({"type": "blob", "bytes_hex": hex(value)}),
    }
}

fn query(connection: &Connection, sql: &str) -> Result<Value> {
    let mut statement = connection.prepare(sql)?;
    let columns: Vec<_> = statement
        .column_names()
        .iter()
        .map(|name| name.to_string())
        .collect();
    let count = statement.column_count();
    let mut rows = statement.query([])?;
    let mut values = Vec::new();
    while let Some(row) = rows.next()? {
        let mut value = Vec::with_capacity(count);
        for index in 0..count {
            value.push(typed(row.get_ref(index)?));
        }
        values.push(value);
    }
    // Canonical multiset; preserves duplicate rows and SQLite value types.
    values.sort_by_cached_key(|row| serde_json::to_string(row).expect("JSON value serialization"));
    Ok(json!({"columns": columns, "rows": values}))
}

fn capture(connection: &Connection) -> Result<Value> {
    let schema = query(
        connection,
        "SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY type,name",
    )?;
    let tables = {
        let mut statement = connection.prepare(
            "SELECT name FROM sqlite_schema WHERE type='table' AND (name NOT GLOB 'sqlite_*' OR name='sqlite_sequence') ORDER BY name")?;
        statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    let mut table_records = Vec::new();
    for name in tables {
        let quoted = identifier(&name);
        let index_names = {
            let mut statement = connection.prepare(&format!("PRAGMA index_list({quoted})"))?;
            statement
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let mut indexes = Vec::new();
        for index in index_names {
            indexes.push(json!({"name": index, "xinfo": query(connection,
                &format!("PRAGMA index_xinfo({})", identifier(&index)))?}));
        }
        table_records.push(json!({
            "name": name,
            "xinfo": query(connection, &format!("PRAGMA table_xinfo({quoted})"))?,
            "foreign_keys": query(connection, &format!("PRAGMA foreign_key_list({quoted})"))?,
            "index_list": query(connection, &format!("PRAGMA index_list({quoted})"))?,
            "indexes": indexes,
            "data": query(connection, &format!("SELECT * FROM {quoted}"))?,
        }));
    }
    let mut pragmas = serde_json::Map::new();
    for name in [
        "cipher_version",
        "cipher_compatibility",
        "cipher_memory_security",
        "busy_timeout",
        "journal_mode",
        "synchronous",
        "foreign_keys",
        "secure_delete",
        "temp_store",
        "trusted_schema",
        "user_version",
        "application_id",
        "encoding",
    ] {
        pragmas.insert(
            name.to_string(),
            query(connection, &format!("PRAGMA {name}"))?,
        );
    }
    Ok(
        json!({"schema": schema, "tables": table_records, "pragmas": pragmas,
        "foreign_key_check": query(connection, "PRAGMA foreign_key_check")?,
        "integrity_check": query(connection, "PRAGMA integrity_check")?}),
    )
}

fn normalize_reference(capture: &mut Value) {
    for table in capture["tables"].as_array_mut().unwrap() {
        match table["name"].as_str().unwrap() {
            "cgka_schema_migrations" => {
                let rows = table["data"]["rows"].as_array().unwrap();
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], json!({"type":"integer", "value":1}));
                assert_eq!(
                    rows[0][1],
                    json!({"type":"text", "bytes_hex":hex(NAME.as_bytes())})
                );
                assert!(rows[0][2]["value"].as_i64().unwrap() >= 0);
                table["data"]["rows"] = json!([]);
            }
            "chat_presentation_meta" => {
                let epoch = &table["data"]["rows"][0][1];
                assert_eq!(epoch["type"], "blob");
                assert_eq!(epoch["bytes_hex"].as_str().unwrap().len(), 32);
                table["data"]["rows"][0][1] = json!({"type":"blob", "rule":"fresh_randomblob_16"});
            }
            _ => {}
        }
    }
}

// Exact test-only delta from the preserved native-derived oracle.
// No runtime compatibility path or broad schema normalization is introduced.
fn normalized_message_reference(mut expected: Value) -> Value {
    assert_eq!(expected["schema"]["rows"].as_array().unwrap().len(), 314);
    let legacy_name = json!({"type":"text","bytes_hex":hex(b"idx_cgka_messages_legacy_order")});
    let schema = expected["schema"]["rows"].as_array_mut().unwrap();
    let legacy = schema.iter().position(|row| row[1] == legacy_name).unwrap();
    assert_eq!(
        schema.remove(legacy),
        json!([{"bytes_hex":"696e646578","type":"text"},{"bytes_hex":"6964785f63676b615f6d657373616765735f6c65676163795f6f72646572","type":"text"},{"bytes_hex":"63676b615f6d65737361676573","type":"text"},{"bytes_hex":"43524541544520494e444558206964785f63676b615f6d657373616765735f6c65676163795f6f726465720a202020204f4e2063676b615f6d6573736167657328696e736572745f6f72646572292057484552452073746f726167655f666f726d6174203d2031","type":"text"}])
    );
    let message = schema
        .iter_mut()
        .find(|row| row[1] == json!({"type":"text","bytes_hex":hex(b"cgka_messages")}))
        .unwrap();
    assert_eq!(
        *message,
        json!([{"bytes_hex":"7461626c65","type":"text"},{"bytes_hex":"63676b615f6d65737361676573","type":"text"},{"bytes_hex":"63676b615f6d65737361676573","type":"text"},{"bytes_hex":"435245415445205441424c45202263676b615f6d657373616765732220280a20202020696e736572745f6f7264657220494e5445474552205052494d415259204b4559204155544f494e4352454d454e542c0a20202020696420424c4f42204e4f54204e554c4c20554e495155452c0a2020202067726f75705f696420424c4f42204e4f54204e554c4c205245464552454e4345532063676b615f67726f75707328696429204f4e2044454c45544520434153434144452c0a2020202065706f636820494e5445474552204e4f54204e554c4c2c0a20202020737461746520494e5445474552204e4f54204e554c4c2c0a2020202073746f726167655f666f726d617420494e5445474552204e4f54204e554c4c2044454641554c5420312c0a202020207265636f726420424c4f422c0a202020207061796c6f616420424c4f422c0a2020202064656665727265645f7065656c20424c4f422c0a20202020434845434b20280a20202020202020202873746f726167655f666f726d6174203d203120414e44207265636f7264204953204e4f54204e554c4c20414e44207061796c6f6164204953204e554c4c290a20202020202020204f520a20202020202020202873746f726167655f666f726d6174203d203220414e44207265636f7264204953204e554c4c20414e44207061796c6f6164204953204e4f54204e554c4c290a20202020290a29","type":"text"}])
    );
    *message = json!([{"bytes_hex":"7461626c65","type":"text"},{"bytes_hex":"63676b615f6d65737361676573","type":"text"},{"bytes_hex":"63676b615f6d65737361676573","type":"text"},{"bytes_hex":"435245415445205441424c45202263676b615f6d657373616765732220280a20202020696e736572745f6f7264657220494e5445474552205052494d415259204b4559204155544f494e4352454d454e542c0a20202020696420424c4f42204e4f54204e554c4c20554e495155452c0a2020202067726f75705f696420424c4f42204e4f54204e554c4c205245464552454e4345532063676b615f67726f75707328696429204f4e2044454c45544520434153434144452c0a2020202065706f636820494e5445474552204e4f54204e554c4c2c0a20202020737461746520494e5445474552204e4f54204e554c4c2c0a202020207061796c6f616420424c4f42204e4f54204e554c4c20434845434b28747970656f66287061796c6f616429203d2027626c6f6227292c0a2020202064656665727265645f7065656c20424c4f420a29","type":"text"}]);
    assert_eq!(schema.len(), 313);
    let message = expected["tables"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|table| table["name"] == "cgka_messages")
        .unwrap();
    assert_eq!(
        *message,
        json!({"data":{"columns":["insert_order","id","group_id","epoch","state","storage_format","record","payload","deferred_peel"],"rows":[]},"foreign_keys":{"columns":["id","seq","table","from","to","on_update","on_delete","match"],"rows":[[{"type":"integer","value":0},{"type":"integer","value":0},{"bytes_hex":"63676b615f67726f757073","type":"text"},{"bytes_hex":"67726f75705f6964","type":"text"},{"bytes_hex":"6964","type":"text"},{"bytes_hex":"4e4f20414354494f4e","type":"text"},{"bytes_hex":"43415343414445","type":"text"},{"bytes_hex":"4e4f4e45","type":"text"}]]},"index_list":{"columns":["seq","name","unique","origin","partial"],"rows":[[{"type":"integer","value":0},{"bytes_hex":"6964785f63676b615f6d657373616765735f67726f75705f6f72646572","type":"text"},{"type":"integer","value":0},{"bytes_hex":"63","type":"text"},{"type":"integer","value":0}],[{"type":"integer","value":1},{"bytes_hex":"6964785f63676b615f6d657373616765735f6c65676163795f6f72646572","type":"text"},{"type":"integer","value":0},{"bytes_hex":"63","type":"text"},{"type":"integer","value":1}],[{"type":"integer","value":2},{"bytes_hex":"6964785f63676b615f6d657373616765735f67726f75705f73746174655f65706f6368","type":"text"},{"type":"integer","value":0},{"bytes_hex":"63","type":"text"},{"type":"integer","value":0}],[{"type":"integer","value":3},{"bytes_hex":"6964785f63676b615f6d657373616765735f67726f75705f65706f6368","type":"text"},{"type":"integer","value":0},{"bytes_hex":"63","type":"text"},{"type":"integer","value":0}],[{"type":"integer","value":4},{"bytes_hex":"73716c6974655f6175746f696e6465785f63676b615f6d657373616765735f31","type":"text"},{"type":"integer","value":1},{"bytes_hex":"75","type":"text"},{"type":"integer","value":0}]]},"indexes":[{"name":"idx_cgka_messages_group_order","xinfo":{"columns":["seqno","cid","name","desc","coll","key"],"rows":[[{"type":"integer","value":0},{"type":"integer","value":2},{"bytes_hex":"67726f75705f6964","type":"text"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":1}],[{"type":"integer","value":1},{"type":"integer","value":0},{"bytes_hex":"696e736572745f6f72646572","type":"text"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":1}],[{"type":"integer","value":2},{"type":"integer","value":-1},{"type":"null"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":0}]]}},{"name":"idx_cgka_messages_legacy_order","xinfo":{"columns":["seqno","cid","name","desc","coll","key"],"rows":[[{"type":"integer","value":0},{"type":"integer","value":0},{"bytes_hex":"696e736572745f6f72646572","type":"text"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":1}],[{"type":"integer","value":1},{"type":"integer","value":-1},{"type":"null"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":0}]]}},{"name":"idx_cgka_messages_group_state_epoch","xinfo":{"columns":["seqno","cid","name","desc","coll","key"],"rows":[[{"type":"integer","value":0},{"type":"integer","value":2},{"bytes_hex":"67726f75705f6964","type":"text"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":1}],[{"type":"integer","value":1},{"type":"integer","value":4},{"bytes_hex":"7374617465","type":"text"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":1}],[{"type":"integer","value":2},{"type":"integer","value":3},{"bytes_hex":"65706f6368","type":"text"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":1}],[{"type":"integer","value":3},{"type":"integer","value":-1},{"type":"null"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":0}]]}},{"name":"idx_cgka_messages_group_epoch","xinfo":{"columns":["seqno","cid","name","desc","coll","key"],"rows":[[{"type":"integer","value":0},{"type":"integer","value":2},{"bytes_hex":"67726f75705f6964","type":"text"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":1}],[{"type":"integer","value":1},{"type":"integer","value":3},{"bytes_hex":"65706f6368","type":"text"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":1}],[{"type":"integer","value":2},{"type":"integer","value":0},{"bytes_hex":"696e736572745f6f72646572","type":"text"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":1}],[{"type":"integer","value":3},{"type":"integer","value":-1},{"type":"null"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":0}]]}},{"name":"sqlite_autoindex_cgka_messages_1","xinfo":{"columns":["seqno","cid","name","desc","coll","key"],"rows":[[{"type":"integer","value":0},{"type":"integer","value":1},{"bytes_hex":"6964","type":"text"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":1}],[{"type":"integer","value":1},{"type":"integer","value":-1},{"type":"null"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":0}]]}}],"name":"cgka_messages","xinfo":{"columns":["cid","name","type","notnull","dflt_value","pk","hidden"],"rows":[[{"type":"integer","value":0},{"bytes_hex":"696e736572745f6f72646572","type":"text"},{"bytes_hex":"494e5445474552","type":"text"},{"type":"integer","value":0},{"type":"null"},{"type":"integer","value":1},{"type":"integer","value":0}],[{"type":"integer","value":1},{"bytes_hex":"6964","type":"text"},{"bytes_hex":"424c4f42","type":"text"},{"type":"integer","value":1},{"type":"null"},{"type":"integer","value":0},{"type":"integer","value":0}],[{"type":"integer","value":2},{"bytes_hex":"67726f75705f6964","type":"text"},{"bytes_hex":"424c4f42","type":"text"},{"type":"integer","value":1},{"type":"null"},{"type":"integer","value":0},{"type":"integer","value":0}],[{"type":"integer","value":3},{"bytes_hex":"65706f6368","type":"text"},{"bytes_hex":"494e5445474552","type":"text"},{"type":"integer","value":1},{"type":"null"},{"type":"integer","value":0},{"type":"integer","value":0}],[{"type":"integer","value":4},{"bytes_hex":"7374617465","type":"text"},{"bytes_hex":"494e5445474552","type":"text"},{"type":"integer","value":1},{"type":"null"},{"type":"integer","value":0},{"type":"integer","value":0}],[{"type":"integer","value":5},{"bytes_hex":"73746f726167655f666f726d6174","type":"text"},{"bytes_hex":"494e5445474552","type":"text"},{"type":"integer","value":1},{"bytes_hex":"31","type":"text"},{"type":"integer","value":0},{"type":"integer","value":0}],[{"type":"integer","value":6},{"bytes_hex":"7265636f7264","type":"text"},{"bytes_hex":"424c4f42","type":"text"},{"type":"integer","value":0},{"type":"null"},{"type":"integer","value":0},{"type":"integer","value":0}],[{"type":"integer","value":7},{"bytes_hex":"7061796c6f6164","type":"text"},{"bytes_hex":"424c4f42","type":"text"},{"type":"integer","value":0},{"type":"null"},{"type":"integer","value":0},{"type":"integer","value":0}],[{"type":"integer","value":8},{"bytes_hex":"64656665727265645f7065656c","type":"text"},{"bytes_hex":"424c4f42","type":"text"},{"type":"integer","value":0},{"type":"null"},{"type":"integer","value":0},{"type":"integer","value":0}]]}})
    );
    *message = json!({"data":{"columns":["insert_order","id","group_id","epoch","state","payload","deferred_peel"],"rows":[]},"foreign_keys":{"columns":["id","seq","table","from","to","on_update","on_delete","match"],"rows":[[{"type":"integer","value":0},{"type":"integer","value":0},{"bytes_hex":"63676b615f67726f757073","type":"text"},{"bytes_hex":"67726f75705f6964","type":"text"},{"bytes_hex":"6964","type":"text"},{"bytes_hex":"4e4f20414354494f4e","type":"text"},{"bytes_hex":"43415343414445","type":"text"},{"bytes_hex":"4e4f4e45","type":"text"}]]},"index_list":{"columns":["seq","name","unique","origin","partial"],"rows":[[{"type":"integer","value":0},{"bytes_hex":"6964785f63676b615f6d657373616765735f67726f75705f6f72646572","type":"text"},{"type":"integer","value":0},{"bytes_hex":"63","type":"text"},{"type":"integer","value":0}],[{"type":"integer","value":1},{"bytes_hex":"6964785f63676b615f6d657373616765735f67726f75705f73746174655f65706f6368","type":"text"},{"type":"integer","value":0},{"bytes_hex":"63","type":"text"},{"type":"integer","value":0}],[{"type":"integer","value":2},{"bytes_hex":"6964785f63676b615f6d657373616765735f67726f75705f65706f6368","type":"text"},{"type":"integer","value":0},{"bytes_hex":"63","type":"text"},{"type":"integer","value":0}],[{"type":"integer","value":3},{"bytes_hex":"73716c6974655f6175746f696e6465785f63676b615f6d657373616765735f31","type":"text"},{"type":"integer","value":1},{"bytes_hex":"75","type":"text"},{"type":"integer","value":0}]]},"indexes":[{"name":"idx_cgka_messages_group_order","xinfo":{"columns":["seqno","cid","name","desc","coll","key"],"rows":[[{"type":"integer","value":0},{"type":"integer","value":2},{"bytes_hex":"67726f75705f6964","type":"text"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":1}],[{"type":"integer","value":1},{"type":"integer","value":0},{"bytes_hex":"696e736572745f6f72646572","type":"text"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":1}],[{"type":"integer","value":2},{"type":"integer","value":-1},{"type":"null"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":0}]]}},{"name":"idx_cgka_messages_group_state_epoch","xinfo":{"columns":["seqno","cid","name","desc","coll","key"],"rows":[[{"type":"integer","value":0},{"type":"integer","value":2},{"bytes_hex":"67726f75705f6964","type":"text"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":1}],[{"type":"integer","value":1},{"type":"integer","value":4},{"bytes_hex":"7374617465","type":"text"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":1}],[{"type":"integer","value":2},{"type":"integer","value":3},{"bytes_hex":"65706f6368","type":"text"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":1}],[{"type":"integer","value":3},{"type":"integer","value":-1},{"type":"null"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":0}]]}},{"name":"idx_cgka_messages_group_epoch","xinfo":{"columns":["seqno","cid","name","desc","coll","key"],"rows":[[{"type":"integer","value":0},{"type":"integer","value":2},{"bytes_hex":"67726f75705f6964","type":"text"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":1}],[{"type":"integer","value":1},{"type":"integer","value":3},{"bytes_hex":"65706f6368","type":"text"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":1}],[{"type":"integer","value":2},{"type":"integer","value":0},{"bytes_hex":"696e736572745f6f72646572","type":"text"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":1}],[{"type":"integer","value":3},{"type":"integer","value":-1},{"type":"null"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":0}]]}},{"name":"sqlite_autoindex_cgka_messages_1","xinfo":{"columns":["seqno","cid","name","desc","coll","key"],"rows":[[{"type":"integer","value":0},{"type":"integer","value":1},{"bytes_hex":"6964","type":"text"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":1}],[{"type":"integer","value":1},{"type":"integer","value":-1},{"type":"null"},{"type":"integer","value":0},{"bytes_hex":"42494e415259","type":"text"},{"type":"integer","value":0}]]}}],"name":"cgka_messages","xinfo":{"columns":["cid","name","type","notnull","dflt_value","pk","hidden"],"rows":[[{"type":"integer","value":0},{"bytes_hex":"696e736572745f6f72646572","type":"text"},{"bytes_hex":"494e5445474552","type":"text"},{"type":"integer","value":0},{"type":"null"},{"type":"integer","value":1},{"type":"integer","value":0}],[{"type":"integer","value":1},{"bytes_hex":"6964","type":"text"},{"bytes_hex":"424c4f42","type":"text"},{"type":"integer","value":1},{"type":"null"},{"type":"integer","value":0},{"type":"integer","value":0}],[{"type":"integer","value":2},{"bytes_hex":"67726f75705f6964","type":"text"},{"bytes_hex":"424c4f42","type":"text"},{"type":"integer","value":1},{"type":"null"},{"type":"integer","value":0},{"type":"integer","value":0}],[{"type":"integer","value":3},{"bytes_hex":"65706f6368","type":"text"},{"bytes_hex":"494e5445474552","type":"text"},{"type":"integer","value":1},{"type":"null"},{"type":"integer","value":0},{"type":"integer","value":0}],[{"type":"integer","value":4},{"bytes_hex":"7374617465","type":"text"},{"bytes_hex":"494e5445474552","type":"text"},{"type":"integer","value":1},{"type":"null"},{"type":"integer","value":0},{"type":"integer","value":0}],[{"type":"integer","value":5},{"bytes_hex":"7061796c6f6164","type":"text"},{"bytes_hex":"424c4f42","type":"text"},{"type":"integer","value":1},{"type":"null"},{"type":"integer","value":0},{"type":"integer","value":0}],[{"type":"integer","value":6},{"bytes_hex":"64656665727265645f7065656c","type":"text"},{"bytes_hex":"424c4f42","type":"text"},{"type":"integer","value":0},{"type":"null"},{"type":"integer","value":0},{"type":"integer","value":0}]]}});
    expected
}

#[test]
fn current_baseline_matches_complete_native_reference_and_reopens_exactly() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let key = SqlCipherKey::new(format!("x'{}'", "11".repeat(32)))?;
    let expected =
        normalized_message_reference(serde_json::from_str(include_str!("reference.json"))?);
    let mut epochs = Vec::new();
    for name in ["a", "b"] {
        let path = directory.path().join(format!("current-{name}.sqlite"));
        let storage = SqliteAccountStorage::open_encrypted(&path, &key)?;
        assert_eq!(storage.migration_summary().0, 1);
        let first = capture(&*storage.lock()?)?;
        epochs.push(
            first["tables"]
                .as_array()
                .unwrap()
                .iter()
                .find(|table| table["name"] == "chat_presentation_meta")
                .unwrap()["data"]["rows"][0][1]
                .clone(),
        );
        let mut compared = first.clone();
        normalize_reference(&mut compared);
        assert_eq!(compared, expected);
        storage.close()?;
        let reopened = SqliteAccountStorage::open_encrypted(&path, &key)?;
        assert_eq!(reopened.migration_summary().0, 0);
        assert_eq!(capture(&*reopened.lock()?)?, first);
        reopened.close()?;
    }
    assert_ne!(epochs[0], epochs[1]);
    Ok(())
}

fn setup(connection: &mut Connection) -> StorageResult<usize> {
    install(
        connection,
        #[cfg(feature = "diagnostic-account-open-witness")]
        None,
    )
}

#[test]
fn current_reopen_preserves_committed_domain_rows_and_epoch() -> Result<()> {
    let store = SqliteAccountStorage::in_memory()?;
    let mut connection = store.lock()?;
    connection.execute_batch("INSERT INTO cgka_groups(id, epoch, record) VALUES (x'aa', 1, x'00'); INSERT INTO account_groups(group_id_hex, endpoint, profile_name, updated_at) VALUES ('aa','fixture','chosen name',7);")?;
    let before = capture(&connection)?;
    let changes = connection.total_changes();
    assert_eq!(setup(&mut connection)?, 0);
    assert_eq!(capture(&connection)?, before);
    assert_eq!(connection.total_changes(), changes);
    Ok(())
}

#[test]
fn old_future_extra_missing_and_malformed_markers_are_refused_without_writes() -> Result<()> {
    for sql in [
        "UPDATE cgka_schema_migrations SET name='0001_initial_schema'",
        "UPDATE cgka_schema_migrations SET version=2",
        "INSERT INTO cgka_schema_migrations VALUES (0,'foreign',0)",
        "DELETE FROM cgka_schema_migrations",
        "UPDATE cgka_schema_migrations SET applied_at_unix_seconds=-1",
        "UPDATE cgka_schema_migrations SET applied_at_unix_seconds='malformed'",
    ] {
        let store = SqliteAccountStorage::in_memory()?;
        let mut connection = store.lock()?;
        connection.execute_batch(sql)?;
        let before = capture(&connection)?;
        let changes = connection.total_changes();
        assert!(setup(&mut connection).is_err(), "{sql}");
        assert_eq!(capture(&connection)?, before);
        assert_eq!(connection.total_changes(), changes);
    }
    Ok(())
}

#[test]
fn partial_foreign_and_malformed_schema_is_never_adopted() -> Result<()> {
    for sql in [
        "CREATE TABLE foreign_table(value INTEGER)",
        "CREATE TABLE cgka_schema_migrations(version TEXT)",
        "CREATE TABLE cgka_schema_migrations(version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at_unix_seconds INTEGER NOT NULL)",
    ] {
        let mut connection = Connection::open_in_memory()?;
        connection.execute_batch(sql)?;
        let changes = connection.total_changes();
        assert!(setup(&mut connection).is_err());
        assert_eq!(connection.total_changes(), changes);
    }
    for sql in [
        "CREATE TABLE sqliteXforeign(value INTEGER)",
        "DROP TABLE cgka_schema_migrations",
        "DROP TRIGGER unpin_chat_when_archived",
        "ALTER TABLE cgka_groups ADD COLUMN foreign_column INTEGER",
        "DELETE FROM chat_presentation_meta",
        "DELETE FROM sqlite_sequence WHERE name='cgka_messages'",
        "INSERT INTO sqlite_sequence VALUES ('foreign', 0)",
        "UPDATE avatar_cache_meta SET access_seq='malformed'",
    ] {
        let store = SqliteAccountStorage::in_memory()?;
        let mut connection = store.lock()?;
        // The trigger case uses a name proven present in the frozen oracle.
        connection.execute_batch(sql)?;
        let changes = connection.total_changes();
        assert!(setup(&mut connection).is_err(), "{sql}");
        assert_eq!(connection.total_changes(), changes);
    }
    Ok(())
}

struct FailureReset;
impl Drop for FailureReset {
    fn drop(&mut self) {
        FAILURE.with(|failure| failure.set(None));
    }
}
#[test]
fn setup_failures_roll_back_schema_seeds_and_marker_before_publication() -> Result<()> {
    for step in ["apply", "ledger", "before_commit"] {
        let mut connection = Connection::open_in_memory()?;
        FAILURE.with(|failure| failure.set(Some(step)));
        let reset = FailureReset;
        assert!(setup(&mut connection).is_err());
        drop(reset);
        assert!(connection.is_autocommit());
        assert_eq!(
            connection.query_row("SELECT COUNT(*) FROM sqlite_schema", [], |row| row
                .get::<_, i64>(0))?,
            0
        );
        assert_eq!(setup(&mut connection)?, 1);
        assert_eq!(setup(&mut connection)?, 0);
    }
    Ok(())
}

#[test]
fn message_timeline_reply_lookup_index_is_current() {
    let store = SqliteAccountStorage::in_memory().unwrap();
    let conn = store.lock().unwrap();
    assert!(connection_has_index(
        &conn,
        "message_timeline",
        "idx_message_timeline_reply_lookup"
    ));
}

#[test]
fn chat_notification_settings_table_is_current() {
    let store = SqliteAccountStorage::in_memory().unwrap();
    let conn = store.lock().unwrap();
    assert!(connection_has_column(
        &conn,
        "chat_notification_settings",
        "group_id_hex"
    ));
    assert!(connection_has_column(
        &conn,
        "chat_notification_settings",
        "muted_until_ms"
    ));
}

#[test]
fn chat_list_interaction_state_columns_are_current_with_safe_defaults() {
    let store = SqliteAccountStorage::in_memory().unwrap();
    let conn = store.lock().unwrap();
    assert_eq!(
        column_default(&conn, "conversation_read_state", "manually_marked_unread").as_deref(),
        Some("0")
    );
    assert!(connection_has_column(
        &conn,
        "account_groups",
        "member_count"
    ));
    assert_eq!(
        column_default(&conn, "chat_list_rows", "manually_marked_unread").as_deref(),
        Some("0")
    );
    assert!(connection_has_column(
        &conn,
        "chat_list_rows",
        "last_message_media_json"
    ));
    assert_eq!(
        column_default(&conn, "chat_list_rows", "last_message_delivery_state").as_deref(),
        Some("'not_applicable'")
    );
}

#[test]
fn direct_conversation_members_table_is_current() {
    let store = SqliteAccountStorage::in_memory().unwrap();
    let conn = store.lock().unwrap();
    let exists: i64 = conn
        .query_row(
            "SELECT EXISTS(
                    SELECT 1 FROM sqlite_master
                    WHERE type = 'table' AND name = 'direct_conversation_members'
                 )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(exists, 1);
    assert!(connection_has_column(
        &conn,
        "direct_conversation_members",
        "member_id_hex"
    ));
}

#[test]
fn message_drafts_tables_are_current() {
    let store = SqliteAccountStorage::in_memory().unwrap();
    let conn = store.lock().unwrap();
    assert!(connection_has_column(&conn, "message_drafts", "content"));
    assert!(connection_has_column(
        &conn,
        "message_draft_attachments",
        "plaintext"
    ));
}

#[test]
fn secure_delete_checkpoint_intents_table_is_current() {
    let store = SqliteAccountStorage::in_memory().unwrap();
    let conn = store.lock().unwrap();
    let exists: i64 = conn
        .query_row(
            "SELECT EXISTS(
                    SELECT 1 FROM sqlite_master
                    WHERE type = 'table' AND name = 'secure_delete_checkpoint_intents'
                 )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(exists, 1);
    let columns = conn
        .prepare("PRAGMA table_info(secure_delete_checkpoint_intents)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert!(columns.iter().any(|column| column == "intent_nonce"));
    assert!(!columns.iter().any(|column| column == "generation"));
    assert!(
        !columns
            .iter()
            .any(|column| column == "checkpoint_completed")
    );
    assert!(
        !columns
            .iter()
            .any(|column| column == "created_at_unix_seconds")
    );
}

#[test]
fn group_owned_tables_have_cascading_foreign_keys() {
    let store = SqliteAccountStorage::in_memory().unwrap();
    let conn = store.lock().unwrap();

    for (table, column) in [
        ("cgka_messages", "group_id"),
        ("cgka_queued_outbound", "group_id"),
        ("cgka_member_capabilities", "group_id"),
        ("cgka_convergence_policies", "group_id"),
        ("cgka_member_validation_cache", "group_id"),
        ("cgka_group_snapshots", "group_id"),
        ("cgka_group_state_checkpoints", "group_id"),
        ("cgka_processed_transport_ids", "group_id"),
    ] {
        assert_eq!(
            foreign_key(&conn, table, column),
            Some(("cgka_groups".to_owned(), "CASCADE".to_owned())),
            "{table}.{column} should cascade when a group is deleted"
        );
    }
    assert_eq!(
        foreign_key(&conn, "pending_push_registration_shares", "group_id_hex"),
        Some(("account_groups".to_owned(), "CASCADE".to_owned())),
        "pending shares should cascade when a projection group is deleted"
    );
    assert_eq!(
        foreign_key(&conn, "pending_push_registration_removals", "group_id_hex"),
        None,
        "durable removal intent must survive projection group deletion"
    );
    assert_eq!(
        foreign_key(&conn, "account_recovery_obligations", "group_id"),
        Some(("cgka_groups".to_owned(), "CASCADE".to_owned())),
        "durable recovery intent must survive projection deletion but cascade with its protocol group"
    );
    assert_eq!(
        foreign_key(&conn, "app_epoch_stall_evidence", "group_id"),
        Some(("cgka_groups".to_owned(), "CASCADE".to_owned())),
        "frozen-epoch evidence is bounded by its protocol group and cascades with it"
    );
}

#[test]
fn group_owned_tables_reject_orphan_rows() {
    let store = SqliteAccountStorage::in_memory().unwrap();
    let conn = store.lock().unwrap();
    let orphan_group = vec![0x99_u8; 4];

    assert_foreign_key_error(conn.execute(
        "INSERT INTO cgka_messages (id, group_id, epoch, state, payload)
             VALUES (?1, ?2, 0, 0, ?3)",
        params![vec![0x01_u8; 4], orphan_group, vec![0xAA_u8]],
    ));
    assert_foreign_key_error(conn.execute(
        "INSERT INTO cgka_processed_transport_ids (id, group_id)
             VALUES (?1, ?2)",
        params![vec![0x09_u8; 4], orphan_group],
    ));
    assert_foreign_key_error(conn.execute(
        "INSERT INTO pending_push_registration_shares (
                group_id_hex, token_fingerprint, registration_updated_at_ms,
                queued_at_ms
             ) VALUES ('orphan', 'fingerprint', 1, 1)",
        [],
    ));
    conn.execute(
        "INSERT INTO pending_push_registration_removals (
                group_id_hex, account_label, account_id_hex, platform,
                token_fingerprint, server_pubkey_hex,
                registration_created_at_ms, registration_updated_at_ms,
                queued_at_ms
             ) VALUES ('orphan', 'alice', 'aa', 1, 'fingerprint', 'bb', 1, 1, 1)",
        [],
    )
    .expect("removal intent must not depend on a projection group row");
    assert_foreign_key_error(conn.execute(
        "INSERT INTO cgka_queued_outbound (id, group_id, created_at_ms, record)
             VALUES (?1, ?2, 0, ?3)",
        params![vec![0x02_u8; 4], orphan_group, vec![0xAA_u8]],
    ));
    assert_foreign_key_error(conn.execute(
        "INSERT INTO cgka_member_capabilities (group_id, member_id, capabilities)
             VALUES (?1, ?2, ?3)",
        params![orphan_group, vec![0x03_u8; 4], vec![0xAA_u8]],
    ));
    assert_foreign_key_error(conn.execute(
        "INSERT INTO cgka_convergence_policies (group_id, policy)
             VALUES (?1, ?2)",
        params![orphan_group, vec![0xAA_u8]],
    ));
    assert_foreign_key_error(conn.execute(
        "INSERT INTO cgka_member_validation_cache (group_id, marker)
             VALUES (?1, ?2)",
        params![orphan_group, vec![0xAA_u8]],
    ));
    assert_foreign_key_error(conn.execute(
        "INSERT INTO cgka_group_snapshots (group_id, name, snapshot)
             VALUES (?1, 'anchor', ?2)",
        params![orphan_group, vec![0xAA_u8]],
    ));
}

fn foreign_key(conn: &rusqlite::Connection, table: &str, column: &str) -> Option<(String, String)> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA foreign_key_list({table})"))
        .unwrap();
    stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(6)?,
        ))
    })
    .unwrap()
    .filter_map(std::result::Result::ok)
    .find_map(|(parent_table, from_column, on_delete)| {
        if from_column == column {
            Some((parent_table, on_delete))
        } else {
            None
        }
    })
}

fn assert_foreign_key_error(result: rusqlite::Result<usize>) {
    let err = result.expect_err("orphan insert should fail");
    assert!(
        err.to_string().contains("FOREIGN KEY constraint failed"),
        "unexpected error: {err}"
    );
}

fn connection_has_column(conn: &rusqlite::Connection, table: &str, column: &str) -> bool {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .unwrap();
    stmt.query_map([], |row| row.get::<_, String>("name"))
        .unwrap()
        .any(|name| name.as_deref() == Ok(column))
}

fn column_default(conn: &rusqlite::Connection, table: &str, column: &str) -> Option<String> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .unwrap();
    stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>("name")?,
            row.get::<_, Option<String>>("dflt_value")?,
        ))
    })
    .unwrap()
    .filter_map(std::result::Result::ok)
    .find_map(|(name, default)| if name == column { default } else { None })
}

fn connection_has_index(conn: &rusqlite::Connection, table: &str, index: &str) -> bool {
    let mut stmt = conn
        .prepare(&format!("PRAGMA index_list({table})"))
        .unwrap();
    stmt.query_map([], |row| row.get::<_, String>("name"))
        .unwrap()
        .any(|name| name.as_deref() == Ok(index))
}

#[test]
#[ignore = "owned subprocess helper used only by baseline boundary parents"]
fn baseline_boundary_child() {
    let path = std::env::var_os("MDK_ACCOUNT_SCHEMA_CRASH_DATABASE").expect("owned database path");
    let key = SqlCipherKey::new("current baseline boundary fixture").unwrap();
    let _storage =
        SqliteAccountStorage::open_encrypted(std::path::PathBuf::from(path), &key).unwrap();
    panic!("parent must terminate the child at its observed boundary");
}

struct OwnedBoundaryChild {
    child: std::process::Child,
    cleanup_started: bool,
    reaped: bool,
}
impl OwnedBoundaryChild {
    fn terminate(&mut self) -> std::result::Result<std::process::ExitStatus, String> {
        if self.cleanup_started {
            return Err("owned child cleanup already started; retry refused".into());
        }
        self.cleanup_started = true;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        if let Some(status) = self.child.try_wait().map_err(|error| error.to_string())? {
            self.reaped = true;
            return Ok(status);
        }
        self.child.kill().map_err(|error| error.to_string())?;
        loop {
            if let Some(status) = self.child.try_wait().map_err(|error| error.to_string())? {
                self.reaped = true;
                return Ok(status);
            }
            if std::time::Instant::now() >= deadline {
                return Err("owned child cleanup exceeded its single five-second budget".into());
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}
impl Drop for OwnedBoundaryChild {
    fn drop(&mut self) {
        if self.reaped || self.cleanup_started {
            return;
        }
        if let Err(error) = self.terminate() {
            eprintln!("{error}");
        }
    }
}

#[cfg(unix)]
fn killed_boundary(step: &str, committed: bool) -> Result<()> {
    use std::os::unix::process::ExitStatusExt;
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("boundary.sqlite");
    let ready = directory.path().join("observed.ready");
    let child = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--ignored",
            "--exact",
            "account_schema::tests::baseline_boundary_child",
            "--nocapture",
        ])
        .env("MDK_ACCOUNT_SCHEMA_CRASH_DATABASE", &database)
        .env("MDK_ACCOUNT_SCHEMA_CRASH_STEP", step)
        .env("MDK_ACCOUNT_SCHEMA_CRASH_READY", &ready)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;
    let mut child = OwnedBoundaryChild {
        child,
        cleanup_started: false,
        reaped: false,
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while !ready.exists() {
        if let Some(status) = child.child.try_wait()? {
            child.reaped = true;
            return Err(format!("child exited before boundary: {status}").into());
        }
        if std::time::Instant::now() >= deadline {
            return Err("owned child never observed its baseline boundary".into());
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let status = child.terminate()?;
    assert_eq!(status.signal(), Some(9));
    let key = SqlCipherKey::new("current baseline boundary fixture")?;
    let storage = SqliteAccountStorage::open_encrypted(&database, &key)?;
    assert_eq!(storage.migration_summary().0, usize::from(!committed));
    let first = capture(&*storage.lock()?)?;
    storage.close()?;
    let reopened = SqliteAccountStorage::open_encrypted(&database, &key)?;
    assert_eq!(reopened.migration_summary().0, 0);
    assert_eq!(capture(&*reopened.lock()?)?, first);
    Ok(())
}

#[cfg(unix)]
#[test]
fn process_kill_before_baseline_commit_rolls_back_and_fresh_open_installs_once() -> Result<()> {
    killed_boundary("before_commit", false)
}
#[cfg(unix)]
#[test]
fn process_kill_after_baseline_commit_reopens_the_completed_schema_without_reinstalling()
-> Result<()> {
    killed_boundary("after_commit", true)
}

#[test]
fn committed_current_messages_sequences_and_revisions_survive_file_reopen() -> Result<()> {
    use crate::storage::test_support::{
        gid, mid, sample_group, sample_message, sample_queued_intent,
    };
    use cgka_traits::storage::{
        GroupStorage, MessageStorage, OutboundFanoutStorage, OutboundIntentStorage,
    };
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("current-writes.sqlite");
    let key = SqlCipherKey::new(format!("x'{}'", "22".repeat(32)))?;
    let storage = SqliteAccountStorage::open_encrypted(&path, &key)?;
    assert_eq!(storage.migration_summary().0, 1);
    let group = sample_group(gid(1), 4, 2);
    let message = sample_message(mid(2), gid(1), 4);
    storage.put_group(&group)?;
    storage.put_message(&message)?;
    storage.put_queued_outbound_intent(&sample_queued_intent(mid(3), gid(1)))?;
    storage.put_ingress_dedup_marker(&mid(4))?;
    let fanout = current_sequence_fanout(5);
    storage.put_outbound_fanout(&fanout)?;
    let group_hex = hex(group.id.as_slice());
    storage.lock()?.execute("INSERT INTO account_groups(group_id_hex, endpoint, profile_name, updated_at) VALUES (?1, 'fixture', 'current group', 7)", [&group_hex])?;
    let event = crate::StoredAppEvent {
        group_id_hex: group_hex.clone(),
        message_id_hex: "06".repeat(32),
        source_message_id_hex: Some("07".repeat(32)),
        source_epoch: Some(4),
        direction: "received".into(),
        sender: "08".repeat(32),
        plaintext: "current event".into(),
        kind: cgka_traits::app_event::MARMOT_APP_EVENT_KIND_CHAT,
        tags: Vec::new(),
        recorded_at: 9,
        received_at: 9,
        origin_commit_id: None,
        moderation_grant: false,
    };
    storage.record_app_event(&event)?;
    let submission = crate::LocalSubmission {
        group_id_hex: group_hex.clone(),
        client_token: "current-token".into(),
        message_id_hex: "09".repeat(32),
        request_hash: vec![10; 32],
        payload_hash: vec![11; 32],
        payload: Some(b"current admission".to_vec()),
        request_json: Some("{}".into()),
        state: 0,
        outcome_json: None,
    };
    storage.insert_local_submission(&submission)?;
    let delivery = current_sequence_delivery(12);
    assert_eq!(
        storage.spill_account_deliveries(
            std::slice::from_ref(&delivery),
            crate::DeliverySpillLimits {
                max_rows: 3,
                max_bytes: u64::MAX
            },
            13
        )?,
        vec![crate::DeliverySpillDisposition::Stored]
    );
    let before = {
        let connection = storage.lock()?;
        connection.execute_batch("UPDATE account_recovery_state SET loss_revision=9, route_revision=7, inventory_revision=5; UPDATE account_recovery_comparison SET revision=3, settled_revision=2; UPDATE chat_list_navigation_meta SET revision=13; UPDATE message_draft_revision_clock SET revision=17; UPDATE avatar_cache_meta SET access_seq=19; UPDATE user_block_list SET revision=23;")?;
        let grown: i64 = connection.query_row(
            "SELECT COUNT(*) FROM sqlite_sequence WHERE seq > 0",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(grown, 7);
        capture(&connection)?
    };
    storage.close()?;
    let reopened = SqliteAccountStorage::open_encrypted(&path, &key)?;
    assert_eq!(reopened.migration_summary().0, 0);
    assert_eq!(reopened.get_message(&message.id)?, message);
    assert!(reopened.has_ingress_dedup_marker(&mid(4))?);
    assert_eq!(reopened.outbound_fanout(&mid(5))?, Some(fanout));
    assert_eq!(reopened.app_message_count()?, 1);
    let retained = reopened
        .local_submission(&group_hex, "current-token")?
        .expect("current local admission retained");
    assert_eq!(retained.payload, submission.payload);
    assert_eq!(retained.request_hash, submission.request_hash);
    assert_eq!(
        reopened.spilled_account_deliveries(3, 13)?.deliveries[0].delivery,
        delivery
    );
    assert_eq!(capture(&*reopened.lock()?)?, before);
    reopened.close()?;
    Ok(())
}

#[test]
fn current_normalized_messages_preserve_typed_payloads_and_reopen_exactly() -> Result<()> {
    use crate::storage::test_support::{gid, mid, sample_group, sample_message};
    use cgka_traits::storage::{GroupStorage, MessageStorage};

    let directory = tempfile::tempdir()?;
    let database = directory.path().join("current-normalized-messages.sqlite");
    let key = SqlCipherKey::new(format!("x'{}'", "33".repeat(32)))?;
    let storage = SqliteAccountStorage::open_encrypted(&database, &key)?;
    storage.put_group(&sample_group(gid(1), 4, 2))?;
    let mut binary = sample_message(mid(1), gid(1), 4);
    binary.payload = vec![0, 255, 128, 0, 1];
    let mut empty = sample_message(mid(2), gid(1), 4);
    empty.payload = Vec::new();
    storage.put_message(&binary)?;
    storage.put_message(&empty)?;
    let before = {
        let connection = storage.lock()?;
        let rows = query(
            &connection,
            "SELECT id, group_id, epoch, state, payload, deferred_peel FROM cgka_messages ORDER BY insert_order",
        )?;
        assert_eq!(
            rows["columns"],
            json!([
                "id",
                "group_id",
                "epoch",
                "state",
                "payload",
                "deferred_peel"
            ])
        );
        let values = rows["rows"].as_array().unwrap();
        assert_eq!(values.len(), 2);
        for message in [&binary, &empty] {
            let row = values
                .iter()
                .find(|row| row[0] == json!({"type":"blob","bytes_hex":hex(message.id.as_slice())}))
                .unwrap();
            assert_eq!(
                row[1],
                json!({"type":"blob","bytes_hex":hex(message.group_id.as_slice())})
            );
            assert_eq!(row[2], json!({"type":"integer","value":4}));
            assert_eq!(
                row[4],
                json!({"type":"blob","bytes_hex":hex(&message.payload)})
            );
            assert_eq!(row[5], json!({"type":"null"}));
        }
        let before = capture(&connection)?;
        let changes = connection.total_changes();
        for payload in ["NULL", "'text'", "1", "1.5"] {
            assert!(
                connection
                    .execute(
                        &format!("UPDATE cgka_messages SET payload={payload} WHERE id=?1"),
                        [binary.id.as_slice()]
                    )
                    .is_err()
            );
            assert_eq!(capture(&connection)?, before);
            assert_eq!(connection.total_changes(), changes);
        }
        assert!(
            connection
                .execute(
                    "INSERT INTO cgka_messages(id, group_id, epoch, state) VALUES (?1, ?2, 4, 0)",
                    params![mid(3).as_slice(), gid(1).as_slice()]
                )
                .is_err()
        );
        assert_eq!(capture(&connection)?, before);
        assert_eq!(connection.total_changes(), changes);
        assert_eq!(
            query(&connection, "PRAGMA integrity_check")?["rows"],
            json!([[{"type":"text","bytes_hex":hex(b"ok")}]])
        );
        assert_eq!(
            query(&connection, "PRAGMA foreign_key_check")?["rows"],
            json!([])
        );
        before
    };
    storage.close()?;
    let reopened = SqliteAccountStorage::open_encrypted(&database, &key)?;
    assert_eq!(reopened.migration_summary().0, 0);
    assert_eq!(reopened.get_message(&binary.id)?, binary);
    assert_eq!(reopened.get_message(&empty.id)?, empty);
    assert_eq!(capture(&*reopened.lock()?)?, before);
    reopened.close()?;
    Ok(())
}

fn current_sequence_delivery(id: u8) -> cgka_traits::TransportDelivery {
    use cgka_traits::transport::{TransportEnvelope, TransportMessage, TransportSource};
    use cgka_traits::{
        MemberId, MessageId, TransportDelivery, TransportDeliveryPlane, TransportDeliverySource,
    };

    TransportDelivery {
        account_id: MemberId::new(vec![0xAA; 32]),
        group_id_hint: None,
        message: TransportMessage {
            id: MessageId::new(vec![id; 32]),
            payload: vec![id; 64],
            timestamp: cgka_traits::transport::Timestamp(u64::from(id)),
            causal_deps: Vec::new(),
            source: TransportSource("nostr".to_owned()),
            envelope: TransportEnvelope::GroupMessage {
                transport_group_id: vec![0x42; 32],
            },
        },
        received_at: cgka_traits::transport::Timestamp(7),
        source: TransportDeliverySource {
            transport: TransportSource("nostr".to_owned()),
            plane: TransportDeliveryPlane::Group,
            endpoint: None,
            subscription_id: Some("live".to_owned()),
            wire: None,
        },
    }
}

fn current_sequence_fanout(id: u8) -> cgka_traits::OutboundFanout {
    use crate::storage::test_support::{gid, mid};
    use cgka_traits::engine_state::PendingStateRef;
    use cgka_traits::{
        MemberId, OutboundFanout, Timestamp, TransportEndpoint, TransportEnvelope,
        TransportMessage, TransportPublishRequest, TransportPublishTarget, TransportSource,
    };

    OutboundFanout::stage(
        TransportPublishRequest {
            account_id: MemberId::new(vec![0xA1; 32]),
            message: TransportMessage {
                id: mid(id),
                payload: vec![id; 64],
                timestamp: Timestamp(55),
                causal_deps: Vec::new(),
                source: TransportSource("marmot.transport.nostr".into()),
                envelope: TransportEnvelope::GroupMessage {
                    transport_group_id: vec![0xC3; 32],
                },
            },
            target: TransportPublishTarget::Group {
                group_id: gid(1),
                transport_group_id: vec![0xC3; 32],
                endpoints: vec![
                    TransportEndpoint("wss://one.example".into()),
                    TransportEndpoint("wss://two.example".into()),
                ],
            },
            required_acks: 1,
        },
        Some(PendingStateRef::new(41)),
        Some(gid(1)),
        99,
    )
    .unwrap()
}

#[test]
fn duplicate_foreign_and_malformed_sequence_rows_are_refused_without_changes() -> Result<()> {
    for sql in [
        "INSERT INTO sqlite_sequence(name, seq) VALUES ('cgka_messages', 1)",
        "INSERT INTO sqlite_sequence(name, seq) VALUES ('app_events', -1)",
        "INSERT INTO sqlite_sequence(name, seq) VALUES ('app_events', 'malformed')",
        "INSERT INTO sqlite_sequence(name, seq) VALUES (x'aa', 1)",
        "INSERT INTO sqlite_sequence(name, seq) VALUES ('foreign', 1)",
        "DELETE FROM sqlite_sequence WHERE name='cgka_queued_outbound'",
    ] {
        let store = SqliteAccountStorage::in_memory()?;
        let mut connection = store.lock()?;
        connection.execute_batch(sql)?;
        let before = capture(&connection)?;
        let changes = connection.total_changes();
        assert!(setup(&mut connection).is_err(), "{sql}");
        assert_eq!(capture(&connection)?, before);
        assert_eq!(connection.total_changes(), changes);
    }
    Ok(())
}
