use std::collections::HashMap;
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
    pub fn new(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let mut url = std::path::PathBuf::new();
        url.push(path);

        if url.extension().map_or(true, |ext| ext != "csv") {
            return Err("Bad extension!")?;
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
    pub fn update(&self, new_piece: &S) -> anyhow::Result<()> {
        let all = self.read_all()?;

        let new_xs: Vec<_> = all
            .into_iter()
            .map(|x| {
                if x.id() == new_piece.id() {
                    new_piece.clone()
                } else {
                    x
                }
            })
            .collect();

        self.overwrite(new_xs)?;

        Ok(())
    }
    pub fn update_multiple(&self, new_pieces: Vec<S>) -> anyhow::Result<()> {
        if new_pieces.is_empty() {
            return Ok(());
        }

        let updates: HashMap<<S as HasId>::Id, S> = new_pieces
            .into_iter()
            .map(|piece| (piece.id().clone(), piece))
            .collect();

        let all = self.read_all()?;

        let new_xs: Vec<_> = all
            .into_iter()
            .map(|x| updates.get(&x.id()).cloned().unwrap_or(x))
            .collect();

        self.overwrite(new_xs)?;

        Ok(())
    }
    pub fn delete(&self, id: &<S as HasId>::Id) -> Result<(), Box<dyn std::error::Error>> {
        let all = self.read_all()?;

        let new_xs: Vec<_> = all
            .into_iter()
            .filter_map(|x| if x.id() == *id { None } else { Some(x) })
            .collect();

        self.overwrite(new_xs)?;

        Ok(())
    }
    pub fn get(&self, id: &<S as HasId>::Id) -> Result<Option<S>, Box<dyn std::error::Error>> {
        let all = self.read_all()?;

        Ok(all.into_iter().find(|x| x.id() == *id))
    }
    pub fn member(&self, id: &<S as HasId>::Id) -> Result<bool, Box<dyn std::error::Error>> {
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
    pub fn drop(&self) -> Result<(), Box<dyn std::error::Error>> {
        std::fs::File::create(&self.path)?;

        Ok(())
    }
    pub fn read_all(&self) -> anyhow::Result<Vec<S>> {
        let mut read = csv::Reader::from_path(&self.path)?;
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
    pub fn size(&self) -> Result<usize, Box<dyn std::error::Error>> {
        let xs = self.read_all()?;
        Ok(xs.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default, serde::Serialize, serde::Deserialize, Clone)]
    struct Scaf {
        id: u64,
        ok: bool,
        amount: i32,
    }

    impl HasId for Scaf {
        type Id = u64;

        fn id(&self) -> &Self::Id {
            &self.id
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
    fn test_insert() {
        let db = Rows::<Scaf>::new(&get_path()).unwrap();

        let new_piece = Scaf::default();
        db.insert(&new_piece).unwrap();
        assert!(db.member(&new_piece.id).unwrap());
        assert!(db.get(&new_piece.id).unwrap().is_some());
        assert_eq!(db.read_all().unwrap().len(), 1);

        db.insert(&Scaf::default()).unwrap();
        assert_eq!(db.read_all().unwrap().len(), 2);

        // TODO: test delete and update
        db.delete(&new_piece.id).unwrap();
        assert_eq!(db.read_all().unwrap().len(), 1);
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
