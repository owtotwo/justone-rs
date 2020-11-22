use justone;

mod common;

#[test]
fn it_works() -> justone::Result<()> {
    let test_dir = common::setup()?;
    println!("Test Directory is {}", test_dir.display());
    let mut jo = justone::JustOne::with_full_config(justone::default_hasher_creator(), justone::StrictLevel::Common, true);
    let dups = jo.update(&test_dir)?.duplicates()?;
    common::teardown()?;
    Ok(())
}