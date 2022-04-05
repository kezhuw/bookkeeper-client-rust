fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().build_client(true).compile(
        &["proto/BookkeeperProtocol.proto", "proto/DataFormats.proto", "proto/DbLedgerStorageDataFormats.proto"],
        &["proto"],
    )?;
    Ok(())
}
