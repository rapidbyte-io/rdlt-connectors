//! Hand-rolled pgoutput (logical replication) message parser — protocol
//! version 1, the format `pg_logical_slot_peek_binary_changes` emits with
//! `proto_version '1'`. Same house discipline as the binary-COPY decoder:
//! no dependencies, typed errors, no panics on malformed input, fuzzed
//! (`pgoutput_decode`).
//!
//! Message reference: PostgreSQL docs, "Logical Replication Message
//! Formats". Tuple values arrive TEXT-form under proto v1.

/// Typed parse failure — never a panic (fuzz-pinned).
#[derive(Debug, thiserror::Error)]
#[error("pgoutput: {0}")]
pub struct ParseError(pub String);

fn fail<T>(message: impl Into<String>) -> Result<T, ParseError> {
    Err(ParseError(message.into()))
}

/// One column value inside a tuple.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TupleValue {
    Null,
    /// Unchanged out-of-line (TOAST) value — the update did not carry it
    /// (the row-building layer decides what happens next).
    UnchangedToast,
    /// Text-form value bytes (proto v1 sends text).
    Text(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TupleData {
    pub values: Vec<TupleValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationColumn {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    pub id: u32,
    pub namespace: String,
    pub name: String,
    pub columns: Vec<RelationColumn>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    /// Transaction boundary. The wire carries the commit LSN and xid; the
    /// consumer checkpoints from the peek row's own LSN and does not read
    /// them.
    Begin,
    /// Transaction boundary — same posture as [`Message::Begin`].
    Commit,
    Relation(Relation),
    Insert {
        relation: u32,
        new: TupleData,
    },
    Update {
        relation: u32,
        /// Old key ('K') or full old tuple ('O'), when present.
        old: Option<TupleData>,
        new: TupleData,
    },
    Delete {
        relation: u32,
        /// Old key ('K') or full old tuple ('O').
        old: TupleData,
    },
    Truncate {
        relations: Vec<u32>,
    },
    /// Parsed and carried for completeness; the consumer ignores them.
    Origin,
    Type,
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8], ParseError> {
        let end = self
            .position
            .checked_add(count)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| ParseError("message truncated".into()))?;
        let window = &self.bytes[self.position..end];
        self.position = end;
        Ok(window)
    }

    fn u8(&mut self) -> Result<u8, ParseError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, ParseError> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().expect("2")))
    }

    fn u32(&mut self) -> Result<u32, ParseError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().expect("4")))
    }

    fn i32(&mut self) -> Result<i32, ParseError> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().expect("4")))
    }

    fn u64(&mut self) -> Result<u64, ParseError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().expect("8")))
    }

    fn c_string(&mut self) -> Result<String, ParseError> {
        let rest = &self.bytes[self.position..];
        let terminator = rest
            .iter()
            .position(|byte| *byte == 0)
            .ok_or_else(|| ParseError("unterminated string".into()))?;
        let text = std::str::from_utf8(&rest[..terminator])
            .map_err(|_| ParseError("non-UTF8 identifier".into()))?
            .to_owned();
        self.position += terminator + 1;
        Ok(text)
    }

    fn is_done(&self) -> bool {
        self.position == self.bytes.len()
    }
}

fn tuple(reader: &mut Reader<'_>) -> Result<TupleData, ParseError> {
    let column_count = reader.u16()? as usize;
    // Bound: each column needs at least 1 byte — a hostile count cannot
    // allocate more than the message could carry.
    if column_count > reader.bytes.len() {
        return fail("tuple column count exceeds message");
    }
    let mut values = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        match reader.u8()? {
            b'n' => values.push(TupleValue::Null),
            b'u' => values.push(TupleValue::UnchangedToast),
            b't' => {
                let length = reader.u32()? as usize;
                values.push(TupleValue::Text(reader.take(length)?.to_vec()));
            }
            b'b' => {
                // Binary form only appears under options we do not request;
                // reject rather than misinterpret.
                return fail("binary tuple value (unrequested option)");
            }
            other => return fail(format!("unknown tuple value kind {other:#04x}")),
        }
    }
    Ok(TupleData { values })
}

/// Parse ONE pgoutput message (one `data` cell from the peek function).
pub fn parse(bytes: &[u8]) -> Result<Message, ParseError> {
    let mut reader = Reader { bytes, position: 0 };
    let tag = reader.u8()?;
    let message = match tag {
        b'B' => {
            let _final_lsn = reader.u64()?;
            let _commit_timestamp = reader.u64()?;
            let _transaction_id = reader.u32()?;
            Message::Begin
        }
        b'C' => {
            let _flags = reader.u8()?;
            let _commit_lsn = reader.u64()?;
            let _end_lsn = reader.u64()?;
            let _commit_timestamp = reader.u64()?;
            Message::Commit
        }
        b'O' => {
            let _lsn = reader.u64()?;
            let _name = reader.c_string()?;
            Message::Origin
        }
        b'R' => {
            let id = reader.u32()?;
            let namespace = reader.c_string()?;
            let name = reader.c_string()?;
            // Replica-identity setting ('d'/'n'/'f'/'i'); identity is read
            // from the catalog at preflight, not from the wire.
            let _replica_identity = reader.u8()?;
            let column_count = reader.u16()? as usize;
            if column_count > bytes.len() {
                return fail("relation column count exceeds message");
            }
            let mut columns = Vec::with_capacity(column_count);
            for _ in 0..column_count {
                // Wire order per column: flags (identity-key bit), name,
                // type OID, type modifier. Only the name is retained —
                // column mapping is by name; type/identity facts come from
                // the catalog.
                let _flags = reader.u8()?;
                let name = reader.c_string()?;
                let _type_oid = reader.u32()?;
                let _type_modifier = reader.i32()?;
                columns.push(RelationColumn { name });
            }
            Message::Relation(Relation {
                id,
                namespace,
                name,
                columns,
            })
        }
        b'Y' => {
            let _oid = reader.u32()?;
            let _namespace = reader.c_string()?;
            let _name = reader.c_string()?;
            Message::Type
        }
        b'I' => {
            let relation = reader.u32()?;
            match reader.u8()? {
                b'N' => Message::Insert {
                    relation,
                    new: tuple(&mut reader)?,
                },
                other => return fail(format!("insert: expected 'N', got {other:#04x}")),
            }
        }
        b'U' => {
            let relation = reader.u32()?;
            let mut old = None;
            let marker = reader.u8()?;
            let new = match marker {
                b'K' | b'O' => {
                    old = Some(tuple(&mut reader)?);
                    match reader.u8()? {
                        b'N' => tuple(&mut reader)?,
                        other => return fail(format!("update: expected 'N', got {other:#04x}")),
                    }
                }
                b'N' => tuple(&mut reader)?,
                other => return fail(format!("update: unknown marker {other:#04x}")),
            };
            Message::Update { relation, old, new }
        }
        b'D' => {
            let relation = reader.u32()?;
            match reader.u8()? {
                b'K' | b'O' => Message::Delete {
                    relation,
                    old: tuple(&mut reader)?,
                },
                other => return fail(format!("delete: unknown marker {other:#04x}")),
            }
        }
        b'T' => {
            let relation_count = reader.u32()? as usize;
            if relation_count > bytes.len() {
                return fail("truncate relation count exceeds message");
            }
            let _flags = reader.u8()?;
            let mut relations = Vec::with_capacity(relation_count);
            for _ in 0..relation_count {
                relations.push(reader.u32()?);
            }
            Message::Truncate { relations }
        }
        other => return fail(format!("unknown message tag {other:#04x}")),
    };
    if !reader.is_done() {
        return fail("trailing bytes after message");
    }
    Ok(message)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn c_string(text: &str) -> Vec<u8> {
        let mut bytes = text.as_bytes().to_vec();
        bytes.push(0);
        bytes
    }

    fn text_tuple(values: &[Option<&str>]) -> Vec<u8> {
        let mut bytes = (values.len() as u16).to_be_bytes().to_vec();
        for value in values {
            match value {
                None => bytes.push(b'n'),
                Some(text) => {
                    bytes.push(b't');
                    bytes.extend((text.len() as u32).to_be_bytes());
                    bytes.extend(text.as_bytes());
                }
            }
        }
        bytes
    }

    #[test]
    fn round_trips_the_message_set() {
        // Begin
        let mut begin = vec![b'B'];
        begin.extend(7u64.to_be_bytes());
        begin.extend(0i64.to_be_bytes());
        begin.extend(42u32.to_be_bytes());
        assert_eq!(parse(&begin).unwrap(), Message::Begin);

        // Commit
        let mut commit = vec![b'C', 0];
        commit.extend(7u64.to_be_bytes());
        commit.extend(8u64.to_be_bytes());
        commit.extend(0i64.to_be_bytes());
        assert_eq!(parse(&commit).unwrap(), Message::Commit);

        // Relation with one key column.
        let mut relation = vec![b'R'];
        relation.extend(99u32.to_be_bytes());
        relation.extend(c_string("public"));
        relation.extend(c_string("orders"));
        relation.push(b'd');
        relation.extend(2u16.to_be_bytes());
        relation.push(1); // key column
        relation.extend(c_string("id"));
        relation.extend(20u32.to_be_bytes()); // int8
        relation.extend((-1i32).to_be_bytes());
        relation.push(0);
        relation.extend(c_string("name"));
        relation.extend(25u32.to_be_bytes()); // text
        relation.extend((-1i32).to_be_bytes());
        match parse(&relation).unwrap() {
            Message::Relation(parsed) => {
                assert_eq!(parsed.name, "orders");
                assert_eq!(parsed.columns.len(), 2);
                assert_eq!(parsed.columns[0].name, "id");
                assert_eq!(parsed.columns[1].name, "name");
            }
            other => panic!("{other:?}"),
        }

        // Insert
        let mut insert = vec![b'I'];
        insert.extend(99u32.to_be_bytes());
        insert.push(b'N');
        insert.extend(text_tuple(&[Some("1"), Some("ada")]));
        match parse(&insert).unwrap() {
            Message::Insert { relation: 99, new } => {
                assert_eq!(new.values[0], TupleValue::Text(b"1".to_vec()));
            }
            other => panic!("{other:?}"),
        }

        // Update with old key + unchanged TOAST in the new image.
        let mut update = vec![b'U'];
        update.extend(99u32.to_be_bytes());
        update.push(b'K');
        update.extend(text_tuple(&[Some("1"), None]));
        update.push(b'N');
        let mut new = 2u16.to_be_bytes().to_vec();
        new.push(b't');
        new.extend(1u32.to_be_bytes());
        new.push(b'1');
        new.push(b'u'); // unchanged toast
        update.extend(new);
        match parse(&update).unwrap() {
            Message::Update {
                relation: 99,
                old: Some(_),
                new,
            } => assert_eq!(new.values[1], TupleValue::UnchangedToast),
            other => panic!("{other:?}"),
        }

        // Delete by key.
        let mut delete = vec![b'D'];
        delete.extend(99u32.to_be_bytes());
        delete.push(b'K');
        delete.extend(text_tuple(&[Some("1"), None]));
        assert!(matches!(
            parse(&delete).unwrap(),
            Message::Delete { relation: 99, .. }
        ));

        // Truncate.
        let mut truncate = vec![b'T'];
        truncate.extend(1u32.to_be_bytes());
        truncate.push(0);
        truncate.extend(99u32.to_be_bytes());
        assert_eq!(
            parse(&truncate).unwrap(),
            Message::Truncate {
                relations: vec![99]
            }
        );
    }

    #[test]
    fn malformed_inputs_are_typed_errors_never_panics() {
        for bad in [
            &[][..],
            b"B",
            &[b'B', 0, 0],
            &[b'I', 0, 0, 0, 9, b'X'],
            &[b'U', 0, 0, 0, 9, b'Q'],
            &[b'Z', 1, 2, 3],
            // hostile counts
            &[b'I', 0, 0, 0, 9, b'N', 0xFF, 0xFF],
            &[b'T', 0xFF, 0xFF, 0xFF, 0xFF, 0],
            // trailing garbage
            &[b'O', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xAA],
        ] {
            assert!(parse(bad).is_err(), "{bad:?}");
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 4096, ..ProptestConfig::default() })]

        /// The fuzz property in miniature: arbitrary bytes never panic.
        #[test]
        fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
            let _ = parse(&bytes);
        }
    }
}
