use std::time::Duration;

use bookkeeper_client::prelude::*;
use lazy_static::lazy_static;
use pretty_assertions::assert_eq;
use testcontainers::clients::Cli as DockerCli;
use testcontainers::core::{Healthcheck, WaitFor};
use testcontainers::images::generic::GenericImage;
use testcontainers::Container;

const DOCKER_HOST: &str = "172.17.0.1";
const METADATA_ROOT: &str = "/bookkeeper";

fn zookeeper_image() -> GenericImage {
    let healthcheck = Healthcheck::default()
        .with_cmd(["./bin/zkServer.sh", "status"].iter())
        .with_interval(Duration::from_secs(5))
        .with_retries(30);
    GenericImage::new("zookeeper", "3.7.0").with_healthcheck(healthcheck).with_wait_for(WaitFor::Healthcheck)
}

fn bookkeeper_image(zk_port: u16) -> GenericImage {
    GenericImage::new("apache/bookkeeper", "4.14.3")
        .with_env_var("BOOKIE_HTTP_PORT", "3182")
        .with_env_var("BK_ensemblePlacementPolicy", "org.apache.bookkeeper.client.DefaultEnsemblePlacementPolicy")
        .with_env_var("BK_zkServers", format!("{}:{}", DOCKER_HOST, zk_port))
        .with_env_var("BK_zkLedgersRootPath", METADATA_ROOT)
        .with_wait_for(WaitFor::Healthcheck)
}

fn run_image(image: GenericImage) -> Container<'static, GenericImage> {
    let docker = DockerCli::default();
    let container = docker.run(image);
    unsafe { std::mem::transmute::<Container<'_, _>, Container<'_, _>>(container) }
}

struct Cluster {
    #[allow(dead_code)]
    zookeeper: Container<'static, GenericImage>,
    #[allow(dead_code)]
    bookkeeper: Container<'static, GenericImage>,
    service_uri: String,
    bookie_addrs: String,
}

impl Cluster {
    fn configuration(&self) -> Configuration {
        Configuration::new(self.service_uri.clone()).bookies(self.bookie_addrs.clone())
    }
}

fn start_bookkeeper_cluster() -> Cluster {
    let zookeeper = run_image(zookeeper_image());
    let zk_port = zookeeper.get_host_port(2181);
    let bookkeeper = run_image(bookkeeper_image(zk_port));
    let service_uri = format!("zk://127.0.0.1:{}{}", zk_port, METADATA_ROOT);
    let bookie_addrs = format!("127.0.0.1:{}", bookkeeper.get_host_port(3181));
    Cluster { zookeeper, bookkeeper, service_uri, bookie_addrs }
}

const PASSWORD: &[u8; 7] = b"abcdefg";

const ENTRY_ID0: EntryId = EntryId::MIN;
const ENTRY_ID1: EntryId = unsafe { EntryId::unchecked_from_i64(1) };
const ENTRY_ID2: EntryId = unsafe { EntryId::unchecked_from_i64(2) };

const ENTRY0: &[u8] = b"entry-0";
const ENTRY1: &[u8] = b"entry-1";

lazy_static! {
    static ref CREATE_OPTIONS: CreateOptions =
        CreateOptions::new(1, 1, 1).digest(DigestType::MAC, Some(PASSWORD.to_vec()));
    static ref OPEN_OPTIONS: OpenOptions<'static> = OpenOptions::new(DigestType::MAC, Some(PASSWORD));
}

async fn create_empty_ledger(client: &BookKeeper) -> LedgerId {
    let mut ledger = client.create_ledger(CREATE_OPTIONS.clone()).await.unwrap();
    ledger.close(CloseOptions::default()).await.unwrap();
    ledger.id()
}

#[test_log::test(tokio::test)]
async fn test_ledger_open() {
    let cluster = start_bookkeeper_cluster();

    let config = cluster.configuration();
    let client = BookKeeper::new(config).await.unwrap();

    let ledger_id = create_empty_ledger(&client).await;

    let open_options = OpenOptions::new(DigestType::MAC, Some(PASSWORD));
    client.open_ledger(ledger_id, &open_options).await.unwrap();

    let open_options = OpenOptions::new(DigestType::MAC, None);
    assert_eq!(BkErrorKind::UnauthorizedAccess, client.open_ledger(ledger_id, &open_options).await.unwrap_err().kind());

    let open_options = OpenOptions::new(DigestType::CRC32, Some(PASSWORD));
    assert_eq!(BkErrorKind::UnauthorizedAccess, client.open_ledger(ledger_id, &open_options).await.unwrap_err().kind());

    client.open_ledger(ledger_id, &open_options.administrative()).await.unwrap();
}

#[test_log::test(tokio::test)]
async fn test_ledger_recover() {
    let cluster = start_bookkeeper_cluster();

    let config = cluster.configuration();
    let client = BookKeeper::new(config).await.unwrap();

    let ledger = client.create_ledger(CREATE_OPTIONS.clone()).await.unwrap();
    let ledger_id = ledger.id();
    assert_eq!(ENTRY_ID0, ledger.append(ENTRY0).await.unwrap());
    assert_eq!(ENTRY_ID1, ledger.append(ENTRY1).await.unwrap());

    let recovery_options = OpenOptions::new(DigestType::MAC, Some(PASSWORD)).recovery();
    assert_ledger_entries(&client, ledger_id, vec![ENTRY0, ENTRY1], true, &recovery_options).await;

    assert_eq!(BkErrorKind::LedgerFenced, ledger.append(Default::default()).await.unwrap_err().kind());
}

#[test_log::test(tokio::test)]
async fn test_ledger_read() {
    let cluster = start_bookkeeper_cluster();

    let config = cluster.configuration();
    let client = BookKeeper::new(config).await.unwrap();

    let mut ledger = client.create_ledger(CREATE_OPTIONS.clone()).await.unwrap();
    let ledger_id = ledger.id();

    let reader = client.open_ledger(ledger_id, &OPEN_OPTIONS).await.unwrap();
    let poll_options = PollOptions::new(Duration::from_secs(10)).parallel();

    ledger.append(ENTRY0).await.unwrap();
    assert_eq!(
        BkErrorKind::ReadExceedLastAddConfirmed,
        reader.read(ENTRY_ID0, ENTRY_ID0, None).await.unwrap_err().kind()
    );
    assert_eq!(ENTRY0, reader.poll(ENTRY_ID0, &poll_options).await.unwrap());
    assert_eq!(ENTRY0, reader.read(ENTRY_ID0, ENTRY_ID0, None).await.unwrap()[0]);

    ledger.append(ENTRY1).await.unwrap();
    assert_eq!(
        BkErrorKind::ReadExceedLastAddConfirmed,
        reader.read(ENTRY_ID1, ENTRY_ID1, None).await.unwrap_err().kind()
    );
    assert_eq!(ENTRY1, reader.poll(ENTRY_ID1, &poll_options).await.unwrap());
    assert_eq!(ENTRY1, reader.read(ENTRY_ID1, ENTRY_ID1, None).await.unwrap()[0]);

    assert_eq!(
        BkErrorKind::ReadExceedLastAddConfirmed,
        reader.read(ENTRY_ID2, ENTRY_ID2, None).await.unwrap_err().kind()
    );
    assert_eq!(
        BkErrorKind::EntryNotExisted,
        reader.read_unconfirmed(ENTRY_ID2, ENTRY_ID2, None).await.unwrap_err().kind()
    );

    assert_ledger_entries(&client, ledger_id, vec![ENTRY0, ENTRY1], false, &OPEN_OPTIONS).await;

    ledger.close(CloseOptions::default()).await.unwrap();

    assert_eq!(
        BkErrorKind::ReadExceedLastAddConfirmed,
        reader.poll(ENTRY_ID2, &poll_options).await.unwrap_err().kind()
    );

    let closed_reader = ledger.reader().unwrap();
    assert_eq!(closed_reader.closed(), true);
    assert_eq!(closed_reader.last_add_confirmed(), ENTRY_ID1);
    assert_eq!(
        BkErrorKind::ReadExceedLastAddConfirmed,
        closed_reader.read(ENTRY_ID2, ENTRY_ID2, None).await.unwrap_err().kind()
    );
    assert_eq!(
        BkErrorKind::ReadExceedLastAddConfirmed,
        closed_reader.read_unconfirmed(ENTRY_ID2, ENTRY_ID2, None).await.unwrap_err().kind()
    );

    assert_ledger_entries(&client, ledger_id, vec![ENTRY0, ENTRY1], true, &OPEN_OPTIONS).await;
}

async fn assert_ledger_entries<T: AsRef<[u8]>>(
    client: &BookKeeper,
    ledger_id: LedgerId,
    entries: Vec<T>,
    confirmed: bool,
    open_options: &OpenOptions<'_>,
) {
    let entries: Vec<_> = entries.into_iter().map(|entry| entry.as_ref().to_vec()).collect();
    let reader = client.open_ledger(ledger_id, open_options).await.unwrap();
    let last_entry = EntryId::try_from((entries.len() - 1) as i64).unwrap();

    if confirmed {
        assert_eq!(last_entry, reader.last_add_confirmed());
    }

    for parallel in [false, true] {
        let options = if parallel { ReadOptions::default().parallel() } else { ReadOptions::default() };
        let read_entries = if confirmed {
            reader.read(EntryId::MIN, last_entry, Some(&options)).await.unwrap()
        } else {
            reader.read_unconfirmed(EntryId::MIN, last_entry, Some(&options)).await.unwrap()
        };
        assert_eq!(entries, read_entries);
    }
}

#[test_log::test(tokio::test)]
async fn test_ledger_delete() {
    let cluster = start_bookkeeper_cluster();

    let config = cluster.configuration();
    let client = BookKeeper::new(config).await.unwrap();

    let ledger_id = create_empty_ledger(&client).await;

    client.open_ledger(ledger_id, &OPEN_OPTIONS).await.unwrap();
    client.delete_ledger(ledger_id, Default::default()).await.unwrap();

    assert_eq!(BkErrorKind::LedgerNotExisted, client.open_ledger(ledger_id, &OPEN_OPTIONS).await.unwrap_err().kind());

    assert_eq!(
        BkErrorKind::LedgerNotExisted,
        client.delete_ledger(ledger_id, Default::default()).await.unwrap_err().kind()
    );
}
