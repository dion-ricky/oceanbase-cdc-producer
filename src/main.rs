use std::env;
use std::thread;
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Duration, Utc};
use fake::faker::lorem::en::{Sentence, Words};
use fake::Fake;
use mysql::prelude::Queryable;
use mysql::{params, OptsBuilder, Pool, PooledConn, Statement};
use rand::distributions::{Alphanumeric, DistString};
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng, RngCore};
use serde_json::json;

const INSERT_RATIO_DEFAULT: u32 = 60;
const UPDATE_RATIO_DEFAULT: u32 = 30;
const DELETE_RATIO_DEFAULT: u32 = 10;
const SLEEP_MS_DEFAULT: u64 = 500;
const RECONNECT_DELAY_SECS: u64 = 5;

fn main() {
    let config = Config::from_env();
    println!(
        "starting producer pod={} target={}.{}@{}:{} ratios={}/{}/{} sleep_ms={}",
        config.pod_name,
        config.database,
        config.table,
        config.host,
        config.port,
        config.insert_ratio,
        config.update_ratio,
        config.delete_ratio,
        config.sleep_ms
    );

    let mut rng = thread_rng();
    let mut id_generator = IdGenerator::new(&config.pod_name);
    let mut owned_ids: Vec<u64> = Vec::new();

    loop {
        let mut conn = connect_with_retry(&config);
        let statements = prepare_statements_with_retry(&mut conn, &config);

        loop {
            let operation = choose_operation(&config, owned_ids.is_empty(), &mut rng);
            let result = match operation {
                Operation::Insert => do_insert(&mut conn, &statements.insert, &mut id_generator, &mut owned_ids, &mut rng),
                Operation::Update => do_update(&mut conn, &statements.update, &owned_ids, &mut rng),
                Operation::Delete => do_delete(&mut conn, &statements.delete, &mut owned_ids, &mut rng),
            };

            match result {
                Ok(summary) => println!("{} owned_ids={}", summary, owned_ids.len()),
                Err(error) => {
                    eprintln!("database operation failed: {error}");
                    break;
                }
            }

            thread::sleep(StdDuration::from_millis(config.sleep_ms));
        }
    }
}

#[derive(Debug, Clone)]
struct Config {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
    table: String,
    pod_name: String,
    insert_ratio: u32,
    update_ratio: u32,
    delete_ratio: u32,
    sleep_ms: u64,
}

impl Config {
    fn from_env() -> Self {
        Self {
            host: env_var_or("OB_HOST", "observer-service"),
            port: env_var_or("OB_PORT", "2881").parse().unwrap_or(2881),
            user: env_var_or("OB_USER", "root@test"),
            password: env_var_or("OB_PASSWORD", ""),
            database: env_var_or("OB_DATABASE", "test"),
            table: env_var_or("TABLE_NAME", "all_types_cdc_source"),
            pod_name: env_var_or("POD_NAME", "local-producer"),
            insert_ratio: env_var_or("INSERT_RATIO", &INSERT_RATIO_DEFAULT.to_string())
                .parse()
                .unwrap_or(INSERT_RATIO_DEFAULT),
            update_ratio: env_var_or("UPDATE_RATIO", &UPDATE_RATIO_DEFAULT.to_string())
                .parse()
                .unwrap_or(UPDATE_RATIO_DEFAULT),
            delete_ratio: env_var_or("DELETE_RATIO", &DELETE_RATIO_DEFAULT.to_string())
                .parse()
                .unwrap_or(DELETE_RATIO_DEFAULT),
            sleep_ms: env_var_or("SLEEP_MS", &SLEEP_MS_DEFAULT.to_string())
                .parse()
                .unwrap_or(SLEEP_MS_DEFAULT),
        }
    }
}

fn env_var_or(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

struct Statements {
    insert: Statement,
    update: Statement,
    delete: Statement,
}

fn connect_with_retry(config: &Config) -> PooledConn {
    loop {
        let builder = OptsBuilder::new()
            .ip_or_hostname(Some(config.host.clone()))
            .tcp_port(config.port)
            .user(Some(config.user.clone()))
            .pass(Some(config.password.clone()))
            .db_name(Some(config.database.clone()));

        match Pool::new(builder).and_then(|pool| pool.get_conn()) {
            Ok(conn) => {
                println!(
                    "connected to {}:{} as {} database={}",
                    config.host, config.port, config.user, config.database
                );
                return conn;
            }
            Err(error) => {
                eprintln!(
                    "database not ready yet for {}:{} database={} error={error}",
                    config.host, config.port, config.database
                );
                thread::sleep(StdDuration::from_secs(RECONNECT_DELAY_SECS));
            }
        }
    }
}

fn prepare_statements_with_retry(conn: &mut PooledConn, config: &Config) -> Statements {
    loop {
        match prepare_statements(conn, config) {
            Ok(statements) => return statements,
            Err(error) => {
                eprintln!(
                    "table or statement preparation not ready for {}.{} error={error}",
                    config.database, config.table
                );
                thread::sleep(StdDuration::from_secs(RECONNECT_DELAY_SECS));
            }
        }
    }
}

fn prepare_statements(conn: &mut PooledConn, config: &Config) -> mysql::Result<Statements> {
    let insert_sql = format!(
        "INSERT INTO {} (
            id,
            c_tinyint,
            c_tinyint_unsigned,
            c_boolean,
            c_smallint,
            c_smallint_unsigned,
            c_mediumint,
            c_mediumint_unsigned,
            c_int,
            c_int_unsigned,
            c_bigint,
            c_bigint_unsigned,
            c_decimal,
            c_numeric,
            c_float,
            c_double,
            c_bit,
            c_year,
            c_date,
            c_time,
            c_datetime,
            c_timestamp,
            c_char,
            c_varchar,
            c_binary,
            c_varbinary,
            c_tinytext,
            c_text,
            c_mediumtext,
            c_longtext,
            c_tinyblob,
            c_blob,
            c_mediumblob,
            c_longblob,
            c_json,
            c_enum,
            c_set
        ) VALUES (
            :id,
            :c_tinyint,
            :c_tinyint_unsigned,
            :c_boolean,
            :c_smallint,
            :c_smallint_unsigned,
            :c_mediumint,
            :c_mediumint_unsigned,
            :c_int,
            :c_int_unsigned,
            :c_bigint,
            :c_bigint_unsigned,
            :c_decimal,
            :c_numeric,
            :c_float,
            :c_double,
            :c_bit,
            :c_year,
            :c_date,
            :c_time,
            :c_datetime,
            :c_timestamp,
            :c_char,
            :c_varchar,
            :c_binary,
            :c_varbinary,
            :c_tinytext,
            :c_text,
            :c_mediumtext,
            :c_longtext,
            :c_tinyblob,
            :c_blob,
            :c_mediumblob,
            :c_longblob,
            :c_json,
            :c_enum,
            :c_set
        )",
        config.table
    );
    let update_sql = format!(
        "UPDATE {} SET
            c_boolean = :c_boolean,
            c_int = :c_int,
            c_decimal = :c_decimal,
            c_varchar = :c_varchar,
            c_text = :c_text,
            c_json = :c_json,
            c_enum = :c_enum,
            c_set = :c_set
         WHERE id = :id",
        config.table
    );
    let delete_sql = format!("DELETE FROM {} WHERE id = :id", config.table);

    Ok(Statements {
        insert: conn.prepare(insert_sql)?,
        update: conn.prepare(update_sql)?,
        delete: conn.prepare(delete_sql)?,
    })
}

enum Operation {
    Insert,
    Update,
    Delete,
}

fn choose_operation(config: &Config, owned_ids_empty: bool, rng: &mut impl Rng) -> Operation {
    if owned_ids_empty {
        return Operation::Insert;
    }

    let total = config.insert_ratio + config.update_ratio + config.delete_ratio;
    if total == 0 {
        return Operation::Insert;
    }

    let draw = rng.gen_range(0..total);
    if draw < config.insert_ratio {
        Operation::Insert
    } else if draw < config.insert_ratio + config.update_ratio {
        Operation::Update
    } else {
        Operation::Delete
    }
}

fn do_insert(
    conn: &mut PooledConn,
    statement: &Statement,
    id_generator: &mut IdGenerator,
    owned_ids: &mut Vec<u64>,
    rng: &mut impl Rng,
) -> mysql::Result<String> {
    let row = AllTypesRow::generate(id_generator.next_id(), rng);
    let row_id = row.id;

    conn.exec_drop(
        statement,
        params! {
            "id" => row.id,
            "c_tinyint" => row.c_tinyint,
            "c_tinyint_unsigned" => row.c_tinyint_unsigned,
            "c_boolean" => row.c_boolean,
            "c_smallint" => row.c_smallint,
            "c_smallint_unsigned" => row.c_smallint_unsigned,
            "c_mediumint" => row.c_mediumint,
            "c_mediumint_unsigned" => row.c_mediumint_unsigned,
            "c_int" => row.c_int,
            "c_int_unsigned" => row.c_int_unsigned,
            "c_bigint" => row.c_bigint,
            "c_bigint_unsigned" => row.c_bigint_unsigned,
            "c_decimal" => row.c_decimal,
            "c_numeric" => row.c_numeric,
            "c_float" => row.c_float,
            "c_double" => row.c_double,
            "c_bit" => row.c_bit,
            "c_year" => row.c_year,
            "c_date" => row.c_date,
            "c_time" => row.c_time,
            "c_datetime" => row.c_datetime,
            "c_timestamp" => row.c_timestamp,
            "c_char" => row.c_char,
            "c_varchar" => row.c_varchar,
            "c_binary" => row.c_binary,
            "c_varbinary" => row.c_varbinary,
            "c_tinytext" => row.c_tinytext,
            "c_text" => row.c_text,
            "c_mediumtext" => row.c_mediumtext,
            "c_longtext" => row.c_longtext,
            "c_tinyblob" => row.c_tinyblob,
            "c_blob" => row.c_blob,
            "c_mediumblob" => row.c_mediumblob,
            "c_longblob" => row.c_longblob,
            "c_json" => row.c_json,
            "c_enum" => row.c_enum_value,
            "c_set" => row.c_set_value,
        },
    )?;

    owned_ids.push(row_id);
    Ok(format!("insert id={row_id}"))
}

fn do_update(
    conn: &mut PooledConn,
    statement: &Statement,
    owned_ids: &[u64],
    rng: &mut impl Rng,
) -> mysql::Result<String> {
    let id = owned_ids[rng.gen_range(0..owned_ids.len())];
    let update = UpdatePayload::generate(rng);

    conn.exec_drop(
        statement,
        params! {
            "id" => id,
            "c_boolean" => update.c_boolean,
            "c_int" => update.c_int,
            "c_decimal" => update.c_decimal,
            "c_varchar" => update.c_varchar,
            "c_text" => update.c_text,
            "c_json" => update.c_json,
            "c_enum" => update.c_enum_value,
            "c_set" => update.c_set_value,
        },
    )?;

    Ok(format!("update id={id}"))
}

fn do_delete(
    conn: &mut PooledConn,
    statement: &Statement,
    owned_ids: &mut Vec<u64>,
    rng: &mut impl Rng,
) -> mysql::Result<String> {
    let index = rng.gen_range(0..owned_ids.len());
    let id = owned_ids[index];
    conn.exec_drop(statement, params! { "id" => id })?;
    owned_ids.swap_remove(index);
    Ok(format!("delete id={id}"))
}

struct AllTypesRow {
    id: u64,
    c_tinyint: i8,
    c_tinyint_unsigned: u8,
    c_boolean: bool,
    c_smallint: i16,
    c_smallint_unsigned: u16,
    c_mediumint: i32,
    c_mediumint_unsigned: u32,
    c_int: i32,
    c_int_unsigned: u32,
    c_bigint: i64,
    c_bigint_unsigned: u64,
    c_decimal: String,
    c_numeric: String,
    c_float: f32,
    c_double: f64,
    c_bit: Vec<u8>,
    c_year: u16,
    c_date: String,
    c_time: String,
    c_datetime: String,
    c_timestamp: String,
    c_char: String,
    c_varchar: String,
    c_binary: Vec<u8>,
    c_varbinary: Vec<u8>,
    c_tinytext: String,
    c_text: String,
    c_mediumtext: String,
    c_longtext: String,
    c_tinyblob: Vec<u8>,
    c_blob: Vec<u8>,
    c_mediumblob: Vec<u8>,
    c_longblob: Vec<u8>,
    c_json: String,
    c_enum_value: String,
    c_set_value: String,
}

impl AllTypesRow {
    fn generate(id: u64, rng: &mut impl Rng) -> Self {
        let timestamp = random_timestamp(rng);
        let short_words: Vec<String> = Words(2..4).fake();
        let medium_words: Vec<String> = Words(5..9).fake();
        let char_seed = random_ascii(rng, 12);
        let json_tags = pick_set_members(rng).into_iter().map(String::from).collect::<Vec<_>>();

        Self {
            id,
            c_tinyint: rng.gen_range(i8::MIN..=i8::MAX),
            c_tinyint_unsigned: rng.gen_range(u8::MIN..=u8::MAX),
            c_boolean: rng.gen_bool(0.5),
            c_smallint: rng.gen_range(i16::MIN..=i16::MAX),
            c_smallint_unsigned: rng.gen_range(u16::MIN..=u16::MAX),
            c_mediumint: rng.gen_range(-8_388_608..=8_388_607),
            c_mediumint_unsigned: rng.gen_range(0..=16_777_215),
            c_int: rng.gen_range(-2_000_000_000..=2_000_000_000),
            c_int_unsigned: rng.gen_range(0..=4_000_000_000),
            c_bigint: rng.gen_range(-9_000_000_000_000_000_000..=9_000_000_000_000_000_000),
            c_bigint_unsigned: rng.gen_range(1_000_000_000..=u64::MAX / 1024),
            c_decimal: format_decimal(rng, 18, 10),
            c_numeric: format_decimal(rng, 14, 6),
            c_float: rng.gen_range(-10_000.0..10_000.0),
            c_double: rng.gen_range(-1_000_000.0..1_000_000.0),
            c_bit: vec![rng.gen()],
            c_year: rng.gen_range(2000..=2035),
            c_date: timestamp.format("%Y-%m-%d").to_string(),
            c_time: random_time_string(rng),
            c_datetime: timestamp.format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
            c_timestamp: timestamp.format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
            c_char: format!("{: <16}", char_seed),
            c_varchar: format!("{}-{}", short_words.join("-"), random_ascii(rng, 10)),
            c_binary: random_bytes(rng, 16),
            c_varbinary: random_bytes(rng, rng.gen_range(12..48)),
            c_tinytext: Sentence(3..6).fake(),
            c_text: Sentence(8..14).fake(),
            c_mediumtext: medium_words.join(" "),
            c_longtext: format!("{} {}", Sentence(10..18).fake::<String>(), Sentence(10..18).fake::<String>()),
            c_tinyblob: random_bytes(rng, 32),
            c_blob: random_bytes(rng, 128),
            c_mediumblob: random_bytes(rng, 512),
            c_longblob: random_bytes(rng, 2048),
            c_json: json!({
                "source": "oceanbase-cdc-producer",
                "id": id,
                "pod": random_ascii(rng, 6),
                "active": rng.gen_bool(0.5),
                "score": rng.gen_range(0..10_000),
                "tags": json_tags,
            })
            .to_string(),
            c_enum_value: random_enum_value(rng).to_string(),
            c_set_value: random_set_value(rng),
        }
    }
}

struct UpdatePayload {
    c_boolean: bool,
    c_int: i32,
    c_decimal: String,
    c_varchar: String,
    c_text: String,
    c_json: String,
    c_enum_value: String,
    c_set_value: String,
}

impl UpdatePayload {
    fn generate(rng: &mut impl Rng) -> Self {
        let status = random_enum_value(rng);
        Self {
            c_boolean: rng.gen_bool(0.5),
            c_int: rng.gen_range(-2_000_000_000..=2_000_000_000),
            c_decimal: format_decimal(rng, 18, 10),
            c_varchar: format!("mut-{}", random_ascii(rng, 20)),
            c_text: Sentence(8..14).fake(),
            c_json: json!({
                "source": "oceanbase-cdc-producer-update",
                "status": status,
                "updated_at": Utc::now().format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
                "marker": random_ascii(rng, 8),
            })
            .to_string(),
            c_enum_value: status.to_string(),
            c_set_value: random_set_value(rng),
        }
    }
}

struct IdGenerator {
    pod_hash: u16,
    sequence: u8,
}

impl IdGenerator {
    fn new(pod_name: &str) -> Self {
        Self {
            pod_hash: stable_pod_hash(pod_name),
            sequence: 0,
        }
    }

    fn next_id(&mut self) -> u64 {
        self.sequence = self.sequence.wrapping_add(1);
        let now_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after unix epoch")
            .as_millis() as u64;

        // 16 bits pod hash + 40 bits epoch millis + 8 bits local sequence.
        (u64::from(self.pod_hash) << 48) | ((now_millis & 0x00FF_FFFF_FFFF) << 8) | u64::from(self.sequence)
    }
}

fn stable_pod_hash(value: &str) -> u16 {
    let mut hash: u32 = 0x811c9dc5;
    for byte in value.as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x01000193);
    }
    (hash & 0xffff) as u16
}

fn random_timestamp(rng: &mut impl Rng) -> DateTime<Utc> {
    Utc::now()
        - Duration::days(rng.gen_range(0..30))
        - Duration::seconds(rng.gen_range(0..86_400))
        + Duration::microseconds(rng.gen_range(0..1_000_000))
}

fn random_time_string(rng: &mut impl Rng) -> String {
    let hour = rng.gen_range(0..24);
    let minute = rng.gen_range(0..60);
    let second = rng.gen_range(0..60);
    let micros = rng.gen_range(0..1_000_000);
    format!("{hour:02}:{minute:02}:{second:02}.{micros:06}")
}

fn format_decimal(rng: &mut impl Rng, integer_digits: usize, scale: usize) -> String {
    let upper_bound = 10_u64.saturating_pow(integer_digits.min(18) as u32).saturating_sub(1);
    let whole = rng.gen_range(0..=upper_bound.max(1));
    let fraction_upper = 10_u64.saturating_pow(scale.min(18) as u32).saturating_sub(1);
    let fraction = rng.gen_range(0..=fraction_upper.max(1));
    let sign = if rng.gen_bool(0.5) { "" } else { "-" };
    format!("{sign}{whole}.{:0scale$}", fraction, scale = scale)
}

fn random_ascii(rng: &mut impl Rng, len: usize) -> String {
    Alphanumeric.sample_string(rng, len)
}

fn random_bytes(rng: &mut impl Rng, len: usize) -> Vec<u8> {
    let mut bytes = vec![0_u8; len];
    rng.fill_bytes(&mut bytes);
    bytes
}

fn random_enum_value(rng: &mut impl Rng) -> &'static str {
    ["NEW", "PROCESSING", "DONE", "FAILED"]
        .choose(rng)
        .copied()
        .unwrap_or("NEW")
}

fn random_set_value(rng: &mut impl Rng) -> String {
    pick_set_members(rng).join(",")
}

fn pick_set_members(rng: &mut impl Rng) -> Vec<&'static str> {
    let mut values = vec!["A", "B", "C", "D"];
    values.shuffle(rng);
    values.truncate(rng.gen_range(1..=values.len()));
    values
}
