use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter};
use std::{cmp::Ord, fmt::Display};

pub trait HasId {
    type Id: Ord + Display + std::hash::Hash + Clone;

    fn id(&self) -> Self::Id;
}

pub struct Rows<S> {
    path: std::path::PathBuf,
    p1: std::marker::PhantomData<S>,
}

impl<S> Rows<S>
where
    S: serde::Serialize + serde::de::DeserializeOwned + HasId + Clone,
{
    pub fn new(path: &str) -> anyhow::Result<Self> {
        let mut url = std::path::PathBuf::new();
        url.push(path);

        if url.extension().map_or(true, |ext| ext != "csv") {
            return Err(anyhow::anyhow!("Bad extension!"))?;
        }

        // TODO should check file exists and/or is valid?

        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&url)?;

        Ok(Self {
            path: url,
            p1: std::marker::PhantomData,
        })
    }
    pub fn insert(&self, data: S) -> anyhow::Result<()> {
        self.insert_multiple(vec![data])
    }
    pub fn insert_multiple(&self, data: Vec<S>) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let file = std::fs::OpenOptions::new().append(true).open(&self.path)?;
        let meta = file.metadata()?;

        let mut write = csv::WriterBuilder::new()
            .has_headers(meta.len() == 0)
            .from_writer(file);

        for row in data {
            write.serialize(row)?;
        }
        write.flush()?;

        Ok(())
    }
    pub fn update(&self, data: S) -> anyhow::Result<()> {
        self.update_multiple(vec![data])
    }
    pub fn update_multiple(&self, data: Vec<S>) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let updates: HashMap<<S as HasId>::Id, S> = data
            .into_iter()
            .map(|piece| (piece.id().clone(), piece))
            .collect();

        self.with_temp_file(|writer, reader| {
            for result in reader.deserialize() {
                let record: S = result?;
                let updated = updates.get(&record.id()).cloned();
                if let Some(new_record) = updated {
                    writer.serialize(new_record)?;
                } else {
                    writer.serialize(record)?;
                }
            }
            Ok(())
        })
    }
    pub fn delete(&self, id: &<S as HasId>::Id) -> anyhow::Result<()> {
        if !self.member(id)? {
            return Ok(());
        }
        self.with_temp_file(|writer, reader| {
            for result in reader.deserialize() {
                let record: S = result?;
                if record.id() != *id {
                    writer.serialize(record)?;
                }
            }
            Ok(())
        })
    }
    pub fn get(&self, id: &<S as HasId>::Id) -> anyhow::Result<Option<S>> {
        let all = self.read_all()?;

        Ok(all.into_iter().find(|x| x.id() == *id))
    }
    pub fn member(&self, id: &<S as HasId>::Id) -> anyhow::Result<bool> {
        let all = self.read_all()?;

        Ok(all.iter().any(|x| x.id() == *id))
    }
    pub fn overwrite(&self, data: Vec<S>) -> anyhow::Result<()> {
        if data.is_empty() {
            return Err(anyhow::anyhow!("Data is empty!"))?;
        }

        let file = std::fs::File::create(&self.path)?;

        let mut write = csv::Writer::from_writer(file);

        for item in data {
            write.serialize(item)?;
        }
        write.flush()?;

        Ok(())
    }
    pub fn drop(&self) -> anyhow::Result<()> {
        std::fs::File::create(&self.path)?;

        Ok(())
    }
    pub fn read_all(&self) -> anyhow::Result<Vec<S>> {
        let mut read = self.csv_reader()?;
        let rdr = read.deserialize::<S>().collect::<Vec<_>>();
        let res: Result<Vec<_>, _> = rdr.into_iter().collect();
        Ok(res?)
    }
    pub fn read_hashmap(&self) -> anyhow::Result<HashMap<<S as HasId>::Id, S>> {
        let mut read = csv::Reader::from_path(&self.path)?;
        let rdr = read.deserialize::<S>().collect::<Vec<_>>();
        let vec: Result<Vec<_>, _> = rdr.into_iter().collect();
        let res = vec?
            .into_iter()
            .map(|row| (row.id().clone(), row))
            .collect();
        Ok(res)
    }
    pub fn size(&self) -> anyhow::Result<usize> {
        let xs = self.read_all()?;
        Ok(xs.len())
    }

    // private
    fn csv_reader(&self) -> anyhow::Result<csv::Reader<BufReader<File>>> {
        let file = File::open(&self.path)?;
        let reader = csv::Reader::from_reader(BufReader::new(file));
        Ok(reader)
    }
    fn with_temp_file<F>(&self, f: F) -> anyhow::Result<()>
    where
        F: FnOnce(
            &mut csv::Writer<BufWriter<File>>,
            &mut csv::Reader<BufReader<File>>,
        ) -> anyhow::Result<()>,
    {
        let temp_path = self.path.with_extension("csv.tmp");

        let mut reader = self.csv_reader()?;
        let output = File::create(&temp_path)?;
        let mut writer = csv::WriterBuilder::new()
            .has_headers(reader.position().byte() == 0)
            .from_writer(BufWriter::new(output));

        f(&mut writer, &mut reader)?;
        writer.flush()?;

        fs::rename(&temp_path, &self.path)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    #[derive(serde::Serialize, serde::Deserialize, Clone)]
    struct Scaf {
        id: u64,
        ok: bool,
        amount: i32,
    }

    impl Default for Scaf {
        fn default() -> Self {
            Scaf {
                id: rand::rng().random(),
                ok: rand::rng().random(),
                amount: rand::rng().random(),
            }
        }
    }

    impl HasId for Scaf {
        type Id = u64;

        fn id(&self) -> Self::Id {
            self.id
        }
    }

    fn get_path() -> String {
        format!(
            "/tmp/{}.csv",
            std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )
    }

    #[test]
    fn test_create() {
        let db = Rows::<Scaf>::new(&get_path()).unwrap();
        assert_eq!(db.read_all().unwrap().len(), 0);
    }

    #[test]
    fn test_fail() {
        assert!(Rows::<Scaf>::new("").is_err());
        assert!(Rows::<Scaf>::new("data.txt").is_err());
    }

    #[test]
    fn test_insert_delete() {
        let db = Rows::<Scaf>::new(&get_path()).unwrap();

        let new_piece = Scaf::default();
        db.insert(new_piece.clone()).unwrap();
        assert!(db.member(&new_piece.id).unwrap());
        assert!(db.get(&new_piece.id).unwrap().is_some());
        assert_eq!(db.read_all().unwrap().len(), 1);

        db.insert(Scaf::default()).unwrap();
        assert_eq!(db.read_all().unwrap().len(), 2);

        db.insert(Scaf::default()).unwrap();
        assert_eq!(db.read_all().unwrap().len(), 3);

        db.delete(&new_piece.id).unwrap();
        assert_eq!(db.read_all().unwrap().len(), 2);
    }

    #[test]
    fn test_multiple() {
        let db = Rows::<Scaf>::new(&get_path()).unwrap();

        let mut s1 = Scaf::default();
        let mut s2 = Scaf::default();
        let s3 = Scaf::default();
        let s4 = Scaf::default();

        db.insert_multiple(vec![s1.clone(), s2.clone(), s3, s4])
            .unwrap();
        assert_eq!(db.read_all().unwrap().len(), 4);

        s1.amount = 111;
        s2.amount = 222;

        db.update_multiple(vec![s1.clone(), s2.clone()]).unwrap();
        assert_eq!(db.read_all().unwrap().len(), 4);

        assert!(db.get(&s1.id).unwrap().unwrap().amount == 111);
        assert!(db.get(&s2.id).unwrap().unwrap().amount == 222);
    }

    #[test]
    fn test_bulk() {
        let db = Rows::<Scaf>::new(&get_path()).unwrap();

        assert!(db.overwrite(vec![]).is_err());
        assert_eq!(db.read_all().unwrap().len(), 0);

        db.overwrite(vec![Scaf::default()]).unwrap();
        assert_eq!(db.read_all().unwrap().len(), 1);

        db.drop().unwrap();
        assert_eq!(db.read_all().unwrap().len(), 0);
    }
}
